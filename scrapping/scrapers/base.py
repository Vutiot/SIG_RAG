"""Base classes for scrapers with retry logic and error handling."""

import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from utils.rate_limiter import RateLimiter
from utils.state_manager import StateManager
from utils.logger import MetricsLogger

logger = structlog.get_logger(__name__)


class BaseScraper(ABC):
    """Base class for all scrapers."""

    def __init__(
        self,
        task_id: str,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        user_agent: Optional[str] = None,
        timeout: int = 45,
    ):
        """Initialize base scraper.

        Args:
            task_id: Unique task identifier
            state_manager: State manager for resume capability
            rate_limiter: Optional rate limiter
            user_agent: User agent string
            timeout: Request timeout in seconds
        """
        self.task_id = task_id
        self.state_manager = state_manager
        self.rate_limiter = rate_limiter
        self.user_agent = user_agent or "LB-RAG-Agent/1.0"
        self.timeout = timeout
        self.logger = logger.bind(task_id=task_id)
        self.metrics = MetricsLogger(self.logger, task_id)

    @abstractmethod
    def run(self) -> None:
        """Execute the scraping task."""
        pass

    def _should_skip(self, operation_type: str, operation_key: str) -> bool:
        """Check if operation should be skipped (already completed).

        Args:
            operation_type: Type of operation
            operation_key: Unique operation key

        Returns:
            True if should skip, False otherwise
        """
        return self.state_manager.is_operation_completed(
            self.task_id, operation_type, operation_key
        )

    def _mark_completed(
        self,
        operation_type: str,
        operation_key: str,
        metadata: Optional[Dict] = None
    ) -> None:
        """Mark operation as completed.

        Args:
            operation_type: Type of operation
            operation_key: Unique operation key
            metadata: Optional metadata dict
        """
        self.state_manager.record_operation(
            self.task_id, operation_type, operation_key, metadata
        )


class APIClient(BaseScraper):
    """Base class for API clients with pagination support."""

    def __init__(
        self,
        task_id: str,
        base_url: str,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs
    ):
        """Initialize API client.

        Args:
            task_id: Unique task identifier
            base_url: Base URL for API
            state_manager: State manager
            rate_limiter: Rate limiter
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(task_id, state_manager, rate_limiter, **kwargs)
        self.base_url = base_url.rstrip('/')
        self.session = httpx.Client(
            timeout=self.timeout,
            headers={'User-Agent': self.user_agent},
            follow_redirects=True,
        )

    def __del__(self):
        """Clean up session on deletion."""
        if hasattr(self, 'session'):
            self.session.close()

    def _should_retry_error(self, exception: Exception) -> bool:
        """Check if error should be retried.

        Args:
            exception: The exception to check

        Returns:
            True if should retry, False otherwise
        """
        # Always retry timeout errors
        if isinstance(exception, httpx.TimeoutException):
            return True

        # For HTTP errors, don't retry 4xx client errors (except 429 rate limit)
        if isinstance(exception, httpx.HTTPStatusError):
            status_code = exception.response.status_code
            # Don't retry client errors (400-499) except rate limit
            if 400 <= status_code < 500 and status_code != 429:
                return False
            return True

        # Retry other HTTP errors
        return isinstance(exception, httpx.HTTPError)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        retry=lambda retry_state: APIClient._should_retry_error_static(retry_state.outcome.exception()) if retry_state.outcome.failed else False,
        reraise=True,
    )
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        method: str = "GET"
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic.

        Args:
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            method: HTTP method

        Returns:
            Response JSON as dict

        Raises:
            httpx.HTTPError: If request fails after retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Apply rate limiting
        if self.rate_limiter:
            self.rate_limiter.acquire_sync(url)

        self.logger.debug(
            "Making request",
            url=url,
            params=params,
            method=method
        )

        try:
            if method == "GET":
                response = self.session.get(url, params=params)
            elif method == "POST":
                response = self.session.post(url, json=params)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            self.metrics.record_success()
            return response.json()

        except httpx.HTTPError as e:
            status_code = None
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code

            self.logger.error(
                "Request failed",
                url=url,
                error=str(e),
                status_code=status_code
            )
            self.metrics.record_error(type(e).__name__)
            raise

    @staticmethod
    def _should_retry_error_static(exception: Exception) -> bool:
        """Static version of should_retry_error for use in decorator."""
        if exception is None:
            return False
        if isinstance(exception, httpx.TimeoutException):
            return True
        if isinstance(exception, httpx.HTTPStatusError):
            status_code = exception.response.status_code
            if 400 <= status_code < 500 and status_code != 429:
                return False
            return True
        return isinstance(exception, httpx.HTTPError)

    def _paginate_page_size(
        self,
        endpoint: str,
        params: Dict,
        page_param: str = "page",
        size_param: str = "size",
        default_size: int = 1000,
        max_depth: int = 20000,
    ) -> List[Dict]:
        """Paginate through API using page+size parameters.

        Args:
            endpoint: API endpoint
            params: Base parameters
            page_param: Name of page parameter
            size_param: Name of size parameter
            default_size: Page size
            max_depth: Maximum number of results to fetch

        Returns:
            List of all results
        """
        all_results = []
        page = 1
        params = params.copy()
        params[size_param] = default_size

        while True:
            # Create unique operation key including essential query params
            # Sort params for consistent key generation
            param_keys = sorted([k for k in params.keys() if k not in [page_param, size_param]])
            param_str = "&".join([f"{k}={params[k]}" for k in param_keys])
            operation_key = f"{endpoint}?{param_str}&page={page}" if param_str else f"{endpoint}?page={page}"

            if self._should_skip("api_page", operation_key):
                self.logger.info("Skipping completed page", page=page)
                page += 1
                if len(all_results) >= max_depth:
                    break
                continue

            params[page_param] = page

            try:
                response = self._make_request(endpoint, params)
            except httpx.HTTPStatusError as e:
                # Some APIs return 400 when hitting pagination limit
                if e.response.status_code == 400 and page > 1:
                    self.logger.warning(
                        "Hit API pagination limit, stopping gracefully",
                        threshold_type="api_pagination_limit",
                        page=page,
                        total_results=len(all_results)
                    )
                    break
                raise

            # Extract results (structure depends on API)
            results = response.get('data', [])
            if not results:
                break

            all_results.extend(results)
            self._mark_completed("api_page", operation_key)

            self.logger.info(
                "Page fetched",
                page=page,
                results=len(results),
                total=len(all_results)
            )

            # Check stopping conditions
            if len(all_results) >= max_depth:
                self.logger.warning(
                    "Pagination limit reached",
                    threshold_type="max_records",
                    threshold_value=max_depth,
                    records_fetched=len(all_results),
                    page=page
                )
                break
            elif len(results) < default_size:
                break

            page += 1

        # Check for 20k record threshold (API pagination limit indicator)
        if len(all_results) >= 20000:
            self.logger.warning(
                "Hit 20k record threshold",
                threshold_type="record_limit_20k",
                threshold_value=20000,
                records_fetched=len(all_results),
                pages_fetched=page
            )

        self.logger.info(
            "Pagination complete",
            total_results=len(all_results)
        )
        return all_results

    def _paginate_cursor(
        self,
        endpoint: str,
        params: Dict,
        cursor_param: str = "cursor",
        size_param: str = "size",
        default_size: int = 1000,
    ) -> List[Dict]:
        """Paginate through API using cursor-based pagination.

        Args:
            endpoint: API endpoint
            params: Base parameters
            cursor_param: Name of cursor parameter
            size_param: Name of size parameter
            default_size: Page size

        Returns:
            List of all results
        """
        all_results = []
        cursor = None
        page_num = 0
        params = params.copy()
        params[size_param] = default_size

        while True:
            page_num += 1

            # Check if we should skip
            operation_key = f"{endpoint}?cursor={cursor}"
            if self._should_skip("api_cursor", operation_key):
                self.logger.info("Skipping completed cursor page", page=page_num)
                # We can't easily continue with cursor pagination after skip
                # Would need to store cursor values
                break

            if cursor:
                params[cursor_param] = cursor

            response = self._make_request(endpoint, params)

            # Extract results and next cursor
            results = response.get('data', [])
            next_cursor = response.get('next_cursor') or response.get('cursor_next')

            if not results:
                break

            all_results.extend(results)
            self._mark_completed("api_cursor", operation_key, {"cursor": next_cursor})

            self.logger.info(
                "Cursor page fetched",
                page=page_num,
                results=len(results),
                total=len(all_results),
                next_cursor=next_cursor
            )

            if not next_cursor:
                break

            cursor = next_cursor

        self.logger.info("Cursor pagination complete", total_results=len(all_results))
        return all_results


class DownloadScraper(BaseScraper):
    """Base class for download scrapers."""

    def __init__(
        self,
        task_id: str,
        output_dir: Path,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs
    ):
        """Initialize download scraper.

        Args:
            task_id: Unique task identifier
            output_dir: Output directory for downloads
            state_manager: State manager
            rate_limiter: Rate limiter
            **kwargs: Additional arguments for BaseScraper
        """
        super().__init__(task_id, state_manager, rate_limiter, **kwargs)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPError)),
    )
    def download_file(
        self,
        url: str,
        output_path: Optional[Path] = None,
        filename: Optional[str] = None
    ) -> Path:
        """Download file with retry logic.

        Args:
            url: URL to download
            output_path: Full output path (overrides filename)
            filename: Filename (used with output_dir)

        Returns:
            Path to downloaded file

        Raises:
            httpx.HTTPError: If download fails after retries
        """
        # Check if already downloaded
        if self.state_manager.is_downloaded(self.task_id, url):
            self.logger.info("File already downloaded, skipping", url=url)
            return output_path or self.output_dir / filename

        # Determine output path
        if output_path is None:
            if filename is None:
                filename = url.split('/')[-1] or 'download'
            output_path = self.output_dir / filename

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Apply rate limiting
        if self.rate_limiter:
            self.rate_limiter.acquire_sync(url)

        self.logger.info("Downloading file", url=url, output=str(output_path))

        try:
            with httpx.stream(
                "GET",
                url,
                headers={'User-Agent': self.user_agent},
                timeout=self.timeout * 2,  # Double timeout for downloads
                follow_redirects=True,
            ) as response:
                response.raise_for_status()

                with open(output_path, 'wb') as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

            self.metrics.record_success()
            self.state_manager.record_download(self.task_id, url, output_path)

            self.logger.info(
                "Download complete",
                url=url,
                size=output_path.stat().st_size
            )

            return output_path

        except httpx.HTTPError as e:
            self.logger.error("Download failed", url=url, error=str(e))
            self.metrics.record_error(type(e).__name__)
            # Remove partial file if exists
            if output_path.exists():
                output_path.unlink()
            raise
