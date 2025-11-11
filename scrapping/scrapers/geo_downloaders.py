"""Geographic data downloaders.

Handles downloading and extracting:
- Admin Express (communes)
- BD TOPAGE (hydrographic segments)
- ROE (river obstacles)
"""

import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Optional
import geopandas as gpd
import httpx
from bs4 import BeautifulSoup
import structlog

from .base import DownloadScraper
from utils.state_manager import StateManager
from utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)


class GeoDownloader(DownloadScraper):
    """Download and extract geographic data files."""

    def __init__(
        self,
        task_id: str,
        output_dir: Path,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs
    ):
        super().__init__(
            task_id=task_id,
            output_dir=output_dir,
            state_manager=state_manager,
            rate_limiter=rate_limiter,
            **kwargs
        )

    def _find_download_url_from_page(self, page_url: str, extensions: list = ['.zip', '.geojson'], keyword: Optional[str] = None) -> Optional[str]:
        """Parse a data.gouv.fr page to find actual download URL.

        Args:
            page_url: URL to the dataset page
            extensions: List of file extensions to look for
            keyword: Optional keyword to filter resources (e.g., 'commune', 'troncon')

        Returns:
            Direct download URL or None
        """
        # Try data.gouv.fr API first
        if 'data.gouv.fr/datasets/' in page_url:
            try:
                # Extract dataset ID from URL
                dataset_id = page_url.rstrip('/').split('/')[-1]
                api_url = f'https://www.data.gouv.fr/api/1/datasets/{dataset_id}/'

                with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                    response = client.get(api_url, headers={'User-Agent': self.user_agent})
                    response.raise_for_status()
                    data = response.json()

                    # Look through resources
                    for resource in data.get('resources', []):
                        resource_format = resource.get('format', '').lower()
                        resource_title = resource.get('title', '').lower()
                        resource_url = resource.get('url', '')

                        # Check if it matches our criteria
                        format_match = any(ext.lstrip('.') in resource_format for ext in extensions)
                        keyword_match = not keyword or keyword.lower() in resource_title

                        if format_match and keyword_match:
                            self.logger.info("Found resource via API", title=resource['title'], url=resource_url)
                            return resource_url

                    self.logger.warning("No matching resource found via API", dataset_id=dataset_id)

            except Exception as e:
                self.logger.warning("API fetch failed, trying HTML parsing", error=str(e))

        # Fallback to HTML parsing
        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                response = client.get(page_url, headers={'User-Agent': self.user_agent})
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'html.parser')

                # Look for download links
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if any(href.endswith(ext) for ext in extensions):
                        # Make absolute URL
                        if href.startswith('http'):
                            return href
                        elif href.startswith('//'):
                            return 'https:' + href
                        else:
                            from urllib.parse import urljoin
                            return urljoin(page_url, href)

                self.logger.warning("No download link found on page", page_url=page_url)
                return None

        except Exception as e:
            self.logger.error("Failed to parse page", page_url=page_url, error=str(e))
            return None

    def download_and_extract_admin_express(
        self,
        url: str,
        output_geojson: Path
    ) -> Path:
        """Download and extract Admin Express communes.

        Args:
            url: URL to download (can be page URL or direct download)
            output_geojson: Output GeoJSON path

        Returns:
            Path to output GeoJSON file
        """
        if output_geojson.exists() and self.state_manager.is_task_completed(self.task_id):
            self.logger.info("Admin Express already processed", output=str(output_geojson))
            return output_geojson

        self.logger.info("Downloading Admin Express", url=url)

        # Download to temp location
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Check if URL is a page or direct download
            download_url = url
            if 'data.gouv.fr/datasets/' in url or not url.endswith(('.zip', '.geojson', '.gz')):
                self.logger.info("URL appears to be a page, searching for download link")
                found_url = self._find_download_url_from_page(url, keyword='com')  # 'com' matches 'commune'
                if found_url:
                    self.logger.info("Found download URL", download_url=found_url)
                    download_url = found_url
                else:
                    raise ValueError(f"Could not find download link on page: {url}")

            # If download_url is GeoJSON, skip archive extraction
            if download_url.endswith('.geojson'):
                # Download directly to temp file
                geojson_path = self.download_file(download_url, output_path=tmpdir_path / "communes.geojson")

                # Read and save
                gdf = gpd.read_file(geojson_path)

                # Ensure required columns
                if 'code_insee' not in gdf.columns and 'INSEE_COM' in gdf.columns:
                    gdf['code_insee'] = gdf['INSEE_COM']
                if 'nom' not in gdf.columns and 'NOM_COM' in gdf.columns:
                    gdf['nom'] = gdf['NOM_COM']

                output_geojson.parent.mkdir(parents=True, exist_ok=True)
                gdf.to_file(output_geojson, driver='GeoJSON')

                self.logger.info("Admin Express processed", output=str(output_geojson), features=len(gdf))
                return output_geojson

            # Download archive
            archive_path = self.download_file(download_url, output_path=tmpdir_path / "admin_express.zip")

            # Extract
            self.logger.info("Extracting archive")
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdir_path)

            # Find commune shapefile/geojson
            commune_files = list(tmpdir_path.rglob("*commune*.shp")) + \
                           list(tmpdir_path.rglob("*COMMUNE*.shp")) + \
                           list(tmpdir_path.rglob("*commune*.geojson")) + \
                           list(tmpdir_path.rglob("*COMMUNE*.geojson"))

            if not commune_files:
                raise FileNotFoundError("Could not find commune file in archive")

            self.logger.info("Found commune file", file=str(commune_files[0]))

            # Read and convert to GeoJSON
            gdf = gpd.read_file(commune_files[0])

            # Ensure required columns
            if 'code_insee' not in gdf.columns and 'INSEE_COM' in gdf.columns:
                gdf['code_insee'] = gdf['INSEE_COM']
            if 'nom' not in gdf.columns and 'NOM_COM' in gdf.columns:
                gdf['nom'] = gdf['NOM_COM']

            # Save as GeoJSON
            output_geojson.parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(output_geojson, driver='GeoJSON')

            self.logger.info(
                "Admin Express processed",
                output=str(output_geojson),
                features=len(gdf)
            )

        return output_geojson

    def download_and_extract_topage(
        self,
        url: str,
        output_geojson: Path
    ) -> Path:
        """Download and extract BD TOPAGE hydrographic segments.

        Args:
            url: URL to download (can be page URL or direct download)
            output_geojson: Output GeoJSON path

        Returns:
            Path to output GeoJSON file
        """
        if output_geojson.exists() and self.state_manager.is_task_completed(self.task_id):
            self.logger.info("BD TOPAGE already processed", output=str(output_geojson))
            return output_geojson

        self.logger.info("Downloading BD TOPAGE", url=url)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Check if URL is a page or direct download
            download_url = url
            if 'data.gouv.fr/datasets/' in url or not url.endswith(('.zip', '.geojson', '.gz')):
                self.logger.info("URL appears to be a page, searching for download link")
                found_url = self._find_download_url_from_page(url)
                if found_url:
                    self.logger.info("Found download URL", download_url=found_url)
                    download_url = found_url
                else:
                    raise ValueError(f"Could not find download link on page: {url}")

            # Download archive
            archive_path = self.download_file(download_url, output_path=tmpdir_path / "topage.zip")

            # Extract
            self.logger.info("Extracting archive")
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdir_path)

            # Find troncon files
            troncon_files = list(tmpdir_path.rglob("*troncon*.shp")) + \
                           list(tmpdir_path.rglob("*TRONCON*.shp")) + \
                           list(tmpdir_path.rglob("*troncon*.geojson")) + \
                           list(tmpdir_path.rglob("*TRONCON*.geojson"))

            if not troncon_files:
                # Try more generic patterns
                troncon_files = list(tmpdir_path.rglob("*.shp")) + \
                               list(tmpdir_path.rglob("*.geojson"))

            if not troncon_files:
                raise FileNotFoundError("Could not find hydrographic segments file in archive")

            self.logger.info("Found segments file", file=str(troncon_files[0]))

            # Read and convert
            gdf = gpd.read_file(troncon_files[0])

            # Ensure geometry column
            if 'geometry' not in gdf.columns and gdf.geometry.name != 'geometry':
                gdf = gdf.rename_geometry('geometry')

            # Save as GeoJSON
            output_geojson.parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(output_geojson, driver='GeoJSON')

            self.logger.info(
                "BD TOPAGE processed",
                output=str(output_geojson),
                features=len(gdf)
            )

        return output_geojson

    def download_and_extract_roe(
        self,
        url: str,
        output_geojson: Path
    ) -> Path:
        """Download and extract ROE obstacles.

        Args:
            url: URL to download (can be page URL or direct download)
            output_geojson: Output GeoJSON path

        Returns:
            Path to output GeoJSON file
        """
        if output_geojson.exists() and self.state_manager.is_task_completed(self.task_id):
            self.logger.info("ROE already processed", output=str(output_geojson))
            return output_geojson

        self.logger.info("Downloading ROE", url=url)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Check if URL is a page or direct download
            download_url = url
            if ('data.gouv.fr/datasets/' in url or 'sandre.eaufrance.fr' in url) and not url.endswith(('.zip', '.geojson', '.gz')):
                self.logger.info("URL appears to be a page, searching for download link")
                found_url = self._find_download_url_from_page(url)
                if found_url:
                    self.logger.info("Found download URL", download_url=found_url)
                    download_url = found_url
                else:
                    raise ValueError(f"Could not find download link on page: {url}")

            # Determine file type from URL
            if download_url.endswith('.zip'):
                archive_path = self.download_file(download_url, output_path=tmpdir_path / "roe.zip")

                # Extract
                self.logger.info("Extracting archive")
                with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdir_path)

                # Find shapefile/geojson
                obstacle_files = list(tmpdir_path.rglob("*.shp")) + \
                                list(tmpdir_path.rglob("*.geojson"))

                if not obstacle_files:
                    raise FileNotFoundError("Could not find obstacles file in archive")

                source_file = obstacle_files[0]

            elif download_url.endswith('.geojson'):
                source_file = self.download_file(download_url, output_path=tmpdir_path / "roe.geojson")

            else:
                # Try to download as zip
                source_file = self.download_file(download_url, output_path=tmpdir_path / "roe")

            self.logger.info("Found obstacles file", file=str(source_file))

            # Read and convert
            gdf = gpd.read_file(source_file)

            # Ensure geometry column
            if 'geometry' not in gdf.columns and gdf.geometry.name != 'geometry':
                gdf = gdf.rename_geometry('geometry')

            # Save as GeoJSON
            output_geojson.parent.mkdir(parents=True, exist_ok=True)
            gdf.to_file(output_geojson, driver='GeoJSON')

            self.logger.info(
                "ROE processed",
                output=str(output_geojson),
                features=len(gdf)
            )

        return output_geojson

    def run(self) -> None:
        """Not implemented - use specific download methods instead."""
        raise NotImplementedError(
            "Use specific methods like download_and_extract_admin_express() instead"
        )
