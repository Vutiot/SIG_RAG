"""MCP tools for sitemap exploration and web scraping.

Provides tools for:
- Exploring sitemaps
- Finding PDF links on pages
- Testing URLs for accessibility
"""

from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
import structlog

logger = structlog.get_logger(__name__)


def explore_sitemap(
    url: str,
    user_agent: str = "LB-RAG-Agent/1.0",
    timeout: int = 30
) -> Dict:
    """Explore sitemap or sitemap index.

    Args:
        url: URL to sitemap (XML) or sitemap index
        user_agent: User agent string
        timeout: Request timeout in seconds

    Returns:
        Dict with sitemap information and links
    """
    logger.info("Exploring sitemap", url=url)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(
                url,
                headers={'User-Agent': user_agent}
            )
            response.raise_for_status()

            # Parse XML
            soup = BeautifulSoup(response.content, 'xml')

            # Check if it's a sitemap index or regular sitemap
            sitemap_index = soup.find('sitemapindex')

            if sitemap_index:
                # It's a sitemap index
                sitemaps = []
                for sitemap in sitemap_index.find_all('sitemap'):
                    loc = sitemap.find('loc')
                    lastmod = sitemap.find('lastmod')

                    if loc:
                        sitemaps.append({
                            'loc': loc.text,
                            'lastmod': lastmod.text if lastmod else None
                        })

                return {
                    'type': 'sitemap_index',
                    'url': url,
                    'sitemaps': sitemaps,
                    'count': len(sitemaps)
                }

            else:
                # It's a regular sitemap
                urls = []
                urlset = soup.find('urlset')

                if urlset:
                    for url_elem in urlset.find_all('url'):
                        loc = url_elem.find('loc')
                        lastmod = url_elem.find('lastmod')
                        changefreq = url_elem.find('changefreq')
                        priority = url_elem.find('priority')

                        if loc:
                            urls.append({
                                'loc': loc.text,
                                'lastmod': lastmod.text if lastmod else None,
                                'changefreq': changefreq.text if changefreq else None,
                                'priority': priority.text if priority else None
                            })

                return {
                    'type': 'sitemap',
                    'url': url,
                    'urls': urls,
                    'count': len(urls)
                }

    except httpx.HTTPError as e:
        logger.error("Sitemap exploration failed", url=url, error=str(e))
        return {
            'type': 'error',
            'url': url,
            'error': str(e),
            'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
        }
    except Exception as e:
        logger.error("Sitemap parsing failed", url=url, error=str(e))
        return {
            'type': 'error',
            'url': url,
            'error': str(e)
        }


def find_pdf_links(
    url: str,
    user_agent: str = "LB-RAG-Agent/1.0",
    timeout: int = 30,
    include_metadata: bool = True
) -> Dict:
    """Find all PDF links on a page.

    Args:
        url: URL to scan for PDF links
        user_agent: User agent string
        timeout: Request timeout in seconds
        include_metadata: Whether to include link text and attributes

    Returns:
        Dict with PDF links and metadata
    """
    logger.info("Finding PDF links", url=url)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(
                url,
                headers={'User-Agent': user_agent}
            )
            response.raise_for_status()

            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all PDF links
            pdf_links = []

            for link in soup.find_all('a', href=True):
                href = link['href']

                # Check if it's a PDF link
                if href.lower().endswith('.pdf') or '.pdf' in href.lower():
                    # Resolve relative URLs
                    absolute_url = urljoin(url, href)

                    link_data = {
                        'url': absolute_url,
                        'href': href
                    }

                    if include_metadata:
                        # Extract link text
                        link_text = link.get_text(strip=True)
                        link_data['text'] = link_text

                        # Extract title attribute
                        title = link.get('title')
                        if title:
                            link_data['title'] = title

                        # Extract other attributes
                        if link.get('class'):
                            link_data['class'] = ' '.join(link['class'])

                    pdf_links.append(link_data)

            # Also find PDFs in other tags (like embed, object, iframe)
            for tag in soup.find_all(['embed', 'object', 'iframe']):
                src = tag.get('src') or tag.get('data')
                if src and (src.lower().endswith('.pdf') or '.pdf' in src.lower()):
                    absolute_url = urljoin(url, src)
                    pdf_links.append({
                        'url': absolute_url,
                        'href': src,
                        'tag': tag.name
                    })

            return {
                'url': url,
                'pdf_links': pdf_links,
                'count': len(pdf_links)
            }

    except httpx.HTTPError as e:
        logger.error("PDF link search failed", url=url, error=str(e))
        return {
            'url': url,
            'error': str(e),
            'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
            'pdf_links': [],
            'count': 0
        }
    except Exception as e:
        logger.error("PDF link parsing failed", url=url, error=str(e))
        return {
            'url': url,
            'error': str(e),
            'pdf_links': [],
            'count': 0
        }


def test_url(
    url: str,
    user_agent: str = "LB-RAG-Agent/1.0",
    timeout: int = 30,
    method: str = "HEAD"
) -> Dict:
    """Test if URL is accessible.

    Args:
        url: URL to test
        user_agent: User agent string
        timeout: Request timeout in seconds
        method: HTTP method (HEAD or GET)

    Returns:
        Dict with URL status information
    """
    logger.info("Testing URL", url=url, method=method)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            if method.upper() == "HEAD":
                response = client.head(
                    url,
                    headers={'User-Agent': user_agent}
                )
            else:
                response = client.get(
                    url,
                    headers={'User-Agent': user_agent}
                )

            return {
                'url': url,
                'accessible': True,
                'status_code': response.status_code,
                'content_type': response.headers.get('Content-Type'),
                'content_length': response.headers.get('Content-Length'),
                'final_url': str(response.url),
                'redirected': str(response.url) != url
            }

    except httpx.TimeoutException as e:
        logger.warning("URL timeout", url=url, timeout=timeout)
        return {
            'url': url,
            'accessible': False,
            'error': 'timeout',
            'timeout_seconds': timeout
        }
    except httpx.HTTPError as e:
        logger.warning("URL not accessible", url=url, error=str(e))
        return {
            'url': url,
            'accessible': False,
            'error': str(e),
            'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
        }
    except Exception as e:
        logger.error("URL test failed", url=url, error=str(e))
        return {
            'url': url,
            'accessible': False,
            'error': str(e)
        }


def find_all_links(
    url: str,
    user_agent: str = "LB-RAG-Agent/1.0",
    timeout: int = 30,
    filter_domain: Optional[str] = None
) -> Dict:
    """Find all links on a page.

    Args:
        url: URL to scan
        user_agent: User agent string
        timeout: Request timeout in seconds
        filter_domain: Optional domain to filter links (e.g., "example.com")

    Returns:
        Dict with all links
    """
    logger.info("Finding all links", url=url, filter_domain=filter_domain)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.get(
                url,
                headers={'User-Agent': user_agent}
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            links = []

            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(url, href)

                # Filter by domain if specified
                if filter_domain:
                    parsed = urlparse(absolute_url)
                    if filter_domain not in parsed.netloc:
                        continue

                links.append({
                    'url': absolute_url,
                    'href': href,
                    'text': link.get_text(strip=True),
                    'title': link.get('title')
                })

            return {
                'url': url,
                'links': links,
                'count': len(links)
            }

    except httpx.HTTPError as e:
        logger.error("Link search failed", url=url, error=str(e))
        return {
            'url': url,
            'error': str(e),
            'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None,
            'links': [],
            'count': 0
        }
    except Exception as e:
        logger.error("Link parsing failed", url=url, error=str(e))
        return {
            'url': url,
            'error': str(e),
            'links': [],
            'count': 0
        }
