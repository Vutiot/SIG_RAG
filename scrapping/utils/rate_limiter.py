"""Rate limiting utility using token bucket algorithm.

Implements per-domain rate limiting as specified in the playbook.
Thread-safe and async-compatible.
"""

import asyncio
import time
from collections import defaultdict
from threading import Lock
from typing import Dict, Optional
from urllib.parse import urlparse

import structlog

logger = structlog.get_logger(__name__)


class TokenBucket:
    """Token bucket for rate limiting a single domain."""

    def __init__(self, rate: float, burst: Optional[float] = None):
        """Initialize token bucket.

        Args:
            rate: Tokens per second (requests per second)
            burst: Maximum burst size (defaults to rate)
        """
        self.rate = rate
        self.burst = burst or rate
        self.tokens = self.burst
        self.last_update = time.monotonic()
        self.lock = Lock()

    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens from the bucket.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens were available and consumed, False otherwise
        """
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update

            # Add tokens based on elapsed time
            self.tokens = min(
                self.burst,
                self.tokens + elapsed * self.rate
            )
            self.last_update = now

            # Try to consume
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_time(self, tokens: int = 1) -> float:
        """Calculate wait time until tokens are available.

        Args:
            tokens: Number of tokens needed

        Returns:
            Wait time in seconds
        """
        with self.lock:
            if self.tokens >= tokens:
                return 0.0

            tokens_needed = tokens - self.tokens
            return tokens_needed / self.rate

    async def wait_for_token(self, tokens: int = 1) -> None:
        """Async: Wait until tokens are available.

        Args:
            tokens: Number of tokens to wait for
        """
        while not self.consume(tokens):
            wait_time = self.wait_time(tokens)
            await asyncio.sleep(wait_time)

    def wait_for_token_sync(self, tokens: int = 1) -> None:
        """Sync: Wait until tokens are available.

        Args:
            tokens: Number of tokens to wait for
        """
        while not self.consume(tokens):
            wait_time = self.wait_time(tokens)
            time.sleep(wait_time)


class RateLimiter:
    """Multi-domain rate limiter with configurable limits per domain."""

    def __init__(self, rate_limits: Optional[Dict[str, float]] = None):
        """Initialize rate limiter.

        Args:
            rate_limits: Dict mapping domain to max requests per second
                        Example: {"hubeau.eaufrance.fr": 5, "data.gouv.fr": 2}
        """
        self.rate_limits = rate_limits or {}
        self.buckets: Dict[str, TokenBucket] = {}
        self.default_rate = 1.0  # Default 1 req/s if domain not specified
        self.lock = Lock()

        # Initialize buckets for known domains
        for domain, rate in self.rate_limits.items():
            self.buckets[domain] = TokenBucket(rate)

        logger.info(
            "Rate limiter initialized",
            domains=len(self.rate_limits),
            limits=self.rate_limits
        )

    def _get_domain(self, url: str) -> str:
        """Extract domain from URL.

        Args:
            url: Full URL or domain string

        Returns:
            Domain name
        """
        if '://' not in url:
            # Assume it's already a domain
            return url
        parsed = urlparse(url)
        return parsed.netloc

    def _get_bucket(self, domain: str) -> TokenBucket:
        """Get or create token bucket for domain.

        Args:
            domain: Domain name

        Returns:
            Token bucket for this domain
        """
        with self.lock:
            if domain not in self.buckets:
                rate = self.rate_limits.get(domain, self.default_rate)
                self.buckets[domain] = TokenBucket(rate)
                logger.debug("Created bucket for domain", domain=domain, rate=rate)
            return self.buckets[domain]

    async def acquire(self, url: str, tokens: int = 1) -> None:
        """Async: Acquire tokens for a request (waits if necessary).

        Args:
            url: URL or domain to rate limit
            tokens: Number of tokens to acquire (default 1)
        """
        domain = self._get_domain(url)
        bucket = self._get_bucket(domain)
        await bucket.wait_for_token(tokens)
        logger.debug("Token acquired", domain=domain, tokens=tokens)

    def acquire_sync(self, url: str, tokens: int = 1) -> None:
        """Sync: Acquire tokens for a request (waits if necessary).

        Args:
            url: URL or domain to rate limit
            tokens: Number of tokens to acquire (default 1)
        """
        domain = self._get_domain(url)
        bucket = self._get_bucket(domain)
        bucket.wait_for_token_sync(tokens)
        logger.debug("Token acquired", domain=domain, tokens=tokens)

    def try_acquire(self, url: str, tokens: int = 1) -> bool:
        """Try to acquire tokens without waiting.

        Args:
            url: URL or domain to rate limit
            tokens: Number of tokens to acquire (default 1)

        Returns:
            True if tokens were acquired, False otherwise
        """
        domain = self._get_domain(url)
        bucket = self._get_bucket(domain)
        acquired = bucket.consume(tokens)
        if acquired:
            logger.debug("Token acquired", domain=domain, tokens=tokens)
        return acquired

    @classmethod
    def from_playbook(cls, playbook: Dict) -> "RateLimiter":
        """Create rate limiter from playbook configuration.

        Args:
            playbook: Playbook dict with rate_limits section

        Returns:
            Configured RateLimiter instance
        """
        rate_limits = {}
        for limit_config in playbook.get('rate_limits', []):
            domain = limit_config['domain']
            max_rps = limit_config['max_rps']
            rate_limits[domain] = max_rps

        return cls(rate_limits)


# Singleton instance (can be initialized globally)
_global_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> Optional[RateLimiter]:
    """Get the global rate limiter instance."""
    return _global_rate_limiter


def set_rate_limiter(limiter: RateLimiter) -> None:
    """Set the global rate limiter instance."""
    global _global_rate_limiter
    _global_rate_limiter = limiter
