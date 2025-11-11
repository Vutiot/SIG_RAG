"""Scrapy spiders for web crawling and PDF download.

Implements spiders for:
- BSH Loire-Bretagne PDFs
- AELB Quality Documents
"""

import hashlib
import re
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

import scrapy
from scrapy.http import Response
import structlog

from utils.state_manager import StateManager

logger = structlog.get_logger(__name__)


class BSHSpider(scrapy.Spider):
    """Spider for crawling BSH (Bulletin de Situation Hydrologique) PDFs."""

    name = "bsh_loire_bretagne"
    start_urls = [
        "https://www.centre-val-de-loire.developpement-durable.gouv.fr/bulletins-de-situation-hydrologique-bsh-du-bassin-r965.html"
    ]
    allowed_domains = ["centre-val-de-loire.developpement-durable.gouv.fr"]

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 0.5,  # 2 req/s as per playbook
        'FILES_STORE': 'raw/pdfs/bsh/',
        'MEDIA_ALLOW_REDIRECTS': True,
    }

    def __init__(
        self,
        output_dir: str = "raw/pdfs/bsh/",
        state_manager: Optional[StateManager] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_manager = state_manager
        self.pdf_count = 0

    def parse(self, response: Response) -> Generator:
        """Parse BSH index page and follow PDF links.

        Args:
            response: Scrapy response object

        Yields:
            Request or dict items
        """
        # Find all PDF links
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').getall()

        self.logger.info(
            f"Found {len(pdf_links)} PDF links on {response.url}"
        )

        for link in pdf_links:
            pdf_url = response.urljoin(link)

            # Check if already downloaded
            if self.state_manager and self.state_manager.is_downloaded("t7", pdf_url):
                self.logger.info(f"Skipping already downloaded PDF: {pdf_url}")
                continue

            # Generate filename from URL
            filename = self._generate_filename(pdf_url, response)

            yield scrapy.Request(
                url=pdf_url,
                callback=self.save_pdf,
                meta={
                    'filename': filename,
                    'original_url': pdf_url,
                    'source_page': response.url
                }
            )

    def save_pdf(self, response: Response) -> dict:
        """Save PDF file to disk.

        Args:
            response: Response containing PDF data

        Returns:
            Dict with metadata
        """
        filename = response.meta['filename']
        file_path = self.output_dir / filename

        # Save file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(response.body)

        # Calculate hash
        file_hash = hashlib.sha256(response.body).hexdigest()

        self.pdf_count += 1
        self.logger.info(
            f"Downloaded PDF {self.pdf_count}: {filename}",
            size=len(response.body),
            hash=file_hash[:8]
        )

        # Record in state manager
        if self.state_manager:
            self.state_manager.record_download(
                task_id="t7",
                url=response.meta['original_url'],
                local_path=file_path,
                metadata={
                    'source_page': response.meta['source_page'],
                    'content_type': response.headers.get('Content-Type', b'').decode(),
                }
            )

        return {
            'title': filename,
            'url': response.meta['original_url'],
            'local_path': str(file_path),
            'hash': file_hash,
            'size': len(response.body),
            'source': 'bsh_loire_bretagne'
        }

    def _generate_filename(self, url: str, response: Response) -> str:
        """Generate filename from URL and response.

        Args:
            url: PDF URL
            response: Response object

        Returns:
            Filename string
        """
        # Try to extract from URL
        url_filename = url.split('/')[-1]
        if url_filename.endswith('.pdf'):
            return url_filename

        # Generate from URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"bsh_{url_hash}.pdf"


class AELBSpider(scrapy.Spider):
    """Spider for crawling AELB (Agence de l'Eau Loire-Bretagne) quality documents."""

    name = "aelb_qualite_eaux"
    start_urls = [
        "https://donnees-documents.eau-loire-bretagne.fr/home/qualite-des-eaux---informations-et-donnees.html"
    ]
    allowed_domains = [
        "donnees-documents.eau-loire-bretagne.fr",
        "agence.eau-loire-bretagne.fr"
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS': 4,
        'DOWNLOAD_DELAY': 0.5,  # 2 req/s
        'FILES_STORE': 'raw/pdfs/aelb/',
        'MEDIA_ALLOW_REDIRECTS': True,
        'DEPTH_LIMIT': 3,  # Don't go too deep
    }

    def __init__(
        self,
        output_dir: str = "raw/pdfs/aelb/",
        state_manager: Optional[StateManager] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_manager = state_manager
        self.pdf_count = 0
        self.visited_urls = set()

    def parse(self, response: Response) -> Generator:
        """Parse AELB page and follow PDF links and document pages.

        Args:
            response: Scrapy response object

        Yields:
            Request or dict items
        """
        # Mark as visited
        self.visited_urls.add(response.url)

        # Find all PDF links
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').getall()

        # Also find document links
        doc_links = response.css('a[href*="/document/"]::attr(href)').getall()

        self.logger.info(
            f"Found {len(pdf_links)} PDF links and {len(doc_links)} document links on {response.url}"
        )

        # Process PDF links
        for link in pdf_links:
            pdf_url = response.urljoin(link)

            # Check if already downloaded
            if self.state_manager and self.state_manager.is_downloaded("t8", pdf_url):
                continue

            filename = self._generate_filename(pdf_url, response)

            yield scrapy.Request(
                url=pdf_url,
                callback=self.save_pdf,
                meta={
                    'filename': filename,
                    'original_url': pdf_url,
                    'source_page': response.url
                }
            )

        # Follow document pages
        for link in doc_links:
            doc_url = response.urljoin(link)

            # Check domain
            if not any(domain in doc_url for domain in self.allowed_domains):
                continue

            # Avoid revisiting
            if doc_url in self.visited_urls:
                continue

            yield scrapy.Request(
                url=doc_url,
                callback=self.parse,
                dont_filter=False
            )

    def save_pdf(self, response: Response) -> dict:
        """Save PDF file to disk.

        Args:
            response: Response containing PDF data

        Returns:
            Dict with metadata
        """
        filename = response.meta['filename']
        file_path = self.output_dir / filename

        # Save file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(response.body)

        # Calculate hash
        file_hash = hashlib.sha256(response.body).hexdigest()

        self.pdf_count += 1
        self.logger.info(
            f"Downloaded PDF {self.pdf_count}: {filename}",
            size=len(response.body),
            hash=file_hash[:8]
        )

        # Record in state manager
        if self.state_manager:
            self.state_manager.record_download(
                task_id="t8",
                url=response.meta['original_url'],
                local_path=file_path,
                metadata={
                    'source_page': response.meta['source_page'],
                    'content_type': response.headers.get('Content-Type', b'').decode(),
                }
            )

        return {
            'title': filename,
            'url': response.meta['original_url'],
            'local_path': str(file_path),
            'hash': file_hash,
            'size': len(response.body),
            'source': 'aelb_qualite_eaux'
        }

    def _generate_filename(self, url: str, response: Response) -> str:
        """Generate filename from URL and response.

        Args:
            url: PDF URL
            response: Response object

        Returns:
            Filename string
        """
        # Try to extract from URL
        url_filename = url.split('/')[-1]
        if url_filename.endswith('.pdf'):
            # Clean up filename
            url_filename = re.sub(r'[^\w\-_\.]', '_', url_filename)
            return url_filename

        # Try to extract from content-disposition header
        content_disp = response.headers.get('Content-Disposition', b'').decode()
        if 'filename=' in content_disp:
            match = re.search(r'filename="?([^"]+)"?', content_disp)
            if match:
                return match.group(1)

        # Generate from URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"aelb_{url_hash}.pdf"


def run_spider(spider_class, output_dir: str, state_manager: Optional[StateManager] = None) -> None:
    """Run a Scrapy spider.

    Args:
        spider_class: Spider class to run
        output_dir: Output directory
        state_manager: Optional state manager
    """
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    settings = get_project_settings()
    settings.update({
        'LOG_LEVEL': 'INFO',
        'ROBOTSTXT_OBEY': True,
    })

    process = CrawlerProcess(settings)
    process.crawl(
        spider_class,
        output_dir=output_dir,
        state_manager=state_manager
    )
    process.start()
