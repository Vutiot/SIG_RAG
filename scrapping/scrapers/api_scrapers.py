"""API scrapers for Hub'Eau APIs.

Implements scrapers for:
- Hub'Eau Qualité Rivières v2 (4 endpoints)
- Hub'Eau Hydrométrie v2 (4 endpoints)
- Hub'Eau Qualité Nappes v1 (2 endpoints)
"""

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import structlog

from .base import APIClient
from utils.state_manager import StateManager
from utils.rate_limiter import RateLimiter

logger = structlog.get_logger(__name__)


class HubeauQualiteRivieresAPI(APIClient):
    """Scraper for Hub'Eau Qualité Rivières API v2."""

    BASE_URL = "https://hubeau.eaufrance.fr/api/v2/qualite_rivieres"

    def __init__(
        self,
        task_id: str,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs
    ):
        super().__init__(
            task_id=task_id,
            base_url=self.BASE_URL,
            state_manager=state_manager,
            rate_limiter=rate_limiter,
            **kwargs
        )

    def get_stations_pc(
        self,
        code_commune: Optional[str] = None,
        bbox: Optional[str] = None,
        code_masse_eau: Optional[str] = None,
    ) -> List[Dict]:
        """Get physico-chemical stations.

        Args:
            code_commune: Municipality code
            bbox: Bounding box (format: lon_min,lat_min,lon_max,lat_max)
            code_masse_eau: Water body code

        Returns:
            List of station records
        """
        params = {}
        if code_commune:
            params['code_commune'] = code_commune
        if bbox:
            params['bbox'] = bbox
        if code_masse_eau:
            params['code_masse_eau'] = code_masse_eau

        return self._paginate_page_size(
            endpoint='/station_pc',
            params=params,
            max_depth=20000
        )

    def get_operations_pc(
        self,
        code_station: str,
        date_debut_prelevement: Optional[str] = None,
        date_fin_prelevement: Optional[str] = None,
    ) -> List[Dict]:
        """Get sampling operations.

        Args:
            code_station: Station code
            date_debut_prelevement: Start date (YYYY-MM-DD)
            date_fin_prelevement: End date (YYYY-MM-DD)

        Returns:
            List of operation records
        """
        params = {'code_station': code_station}
        if date_debut_prelevement:
            params['date_debut_prelevement'] = date_debut_prelevement
        if date_fin_prelevement:
            params['date_fin_prelevement'] = date_fin_prelevement

        return self._paginate_page_size(
            endpoint='/operation_pc',
            params=params,
            max_depth=20000
        )

    def get_conditions_environnementales(
        self,
        code_operation: str
    ) -> List[Dict]:
        """Get environmental conditions for operations.

        Args:
            code_operation: Operation code

        Returns:
            List of condition records
        """
        params = {'code_operation': code_operation}
        return self._paginate_page_size(
            endpoint='/condition_environnementale_pc',
            params=params,
            max_depth=20000
        )

    def get_analyses_pc(
        self,
        code_parametre: Optional[str] = None,
        libelle_parametre: Optional[str] = None,
        code_station: Optional[str] = None,
        code_commune: Optional[str] = None,
        date_min_prelevement: Optional[str] = None,
        date_max_prelevement: Optional[str] = None,
        fields: Optional[str] = None,
    ) -> List[Dict]:
        """Get physico-chemical analysis results.

        Args:
            code_parametre: Parameter code (e.g., "1340" for NO3)
            libelle_parametre: Parameter name
            code_station: Station code
            code_commune: Municipality code
            date_min_prelevement: Minimum sampling date (YYYY-MM-DD)
            date_max_prelevement: Maximum sampling date (YYYY-MM-DD)
            fields: Comma-separated list of fields to return

        Returns:
            List of analysis records
        """
        params = {}
        if code_parametre:
            params['code_parametre'] = code_parametre
        if libelle_parametre:
            params['libelle_parametre'] = libelle_parametre
        if code_station:
            params['code_station'] = code_station
        if code_commune:
            params['code_commune'] = code_commune
        if date_min_prelevement:
            params['date_min_prelevement'] = date_min_prelevement
        if date_max_prelevement:
            params['date_max_prelevement'] = date_max_prelevement
        if fields:
            params['fields'] = fields

        return self._paginate_page_size(
            endpoint='/analyse_pc',
            params=params,
            max_depth=20000
        )

    def run(self) -> None:
        """Not implemented - use specific methods instead."""
        raise NotImplementedError(
            "Use specific methods like get_analyses_pc() instead"
        )


class HubeauHydrometrieAPI(APIClient):
    """Scraper for Hub'Eau Hydrométrie API v2."""

    BASE_URL = "https://hubeau.eaufrance.fr/api/v2/hydrometrie"

    def __init__(
        self,
        task_id: str,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs
    ):
        super().__init__(
            task_id=task_id,
            base_url=self.BASE_URL,
            state_manager=state_manager,
            rate_limiter=rate_limiter,
            **kwargs
        )

    def get_sites(
        self,
        code_commune_site: Optional[str] = None,
        code_bassin: Optional[str] = None,
        bbox: Optional[str] = None,
    ) -> List[Dict]:
        """Get hydrometric sites.

        Args:
            code_commune_site: Municipality code
            code_bassin: Basin code
            bbox: Bounding box

        Returns:
            List of site records
        """
        params = {}
        if code_commune_site:
            params['code_commune_site'] = code_commune_site
        if code_bassin:
            params['code_bassin'] = code_bassin
        if bbox:
            params['bbox'] = bbox

        return self._paginate_page_size(
            endpoint='/referentiel/sites',
            params=params
        )

    def get_stations(
        self,
        code_commune_station: Optional[str] = None,
        code_site: Optional[str] = None,
        bbox: Optional[str] = None,
    ) -> List[Dict]:
        """Get hydrometric stations.

        Args:
            code_commune_station: Municipality code
            code_site: Site code
            bbox: Bounding box

        Returns:
            List of station records
        """
        params = {}
        if code_commune_station:
            params['code_commune_station'] = code_commune_station
        if code_site:
            params['code_site'] = code_site
        if bbox:
            params['bbox'] = bbox

        return self._paginate_page_size(
            endpoint='/referentiel/stations',
            params=params
        )

    def get_observations_tr(
        self,
        code_station: str,
        date_debut_obs: Optional[str] = None,
        date_fin_obs: Optional[str] = None,
    ) -> List[Dict]:
        """Get real-time observations (limited to 1 month history).

        Args:
            code_station: Station code
            date_debut_obs: Start date (YYYY-MM-DD)
            date_fin_obs: End date (YYYY-MM-DD)

        Returns:
            List of observation records
        """
        params = {'code_station': code_station}
        if date_debut_obs:
            params['date_debut_obs'] = date_debut_obs
        if date_fin_obs:
            params['date_fin_obs'] = date_fin_obs

        return self._paginate_cursor(
            endpoint='/observations_tr',
            params=params
        )

    def get_obs_elab(
        self,
        code_station: Optional[str] = None,
        code_site: Optional[str] = None,
        grandeur_hydro: Optional[str] = None,
        date_debut_obs: Optional[str] = None,
        date_fin_obs: Optional[str] = None,
    ) -> List[Dict]:
        """Get elaborated observations (full history).

        Args:
            code_station: Station code
            code_site: Site code
            grandeur_hydro: Hydrological variable (QmM, QmnJ, etc.)
            date_debut_obs: Start date (YYYY-MM-DD)
            date_fin_obs: End date (YYYY-MM-DD)

        Returns:
            List of observation records
        """
        params = {}
        if code_station:
            params['code_station'] = code_station
        if code_site:
            params['code_site'] = code_site
        if grandeur_hydro:
            params['grandeur_hydro'] = grandeur_hydro
        if date_debut_obs:
            params['date_debut_obs'] = date_debut_obs
        if date_fin_obs:
            params['date_fin_obs'] = date_fin_obs

        return self._paginate_page_size(
            endpoint='/obs_elab',
            params=params
        )

    def run(self) -> None:
        """Not implemented - use specific methods instead."""
        raise NotImplementedError(
            "Use specific methods like get_obs_elab() instead"
        )


class HubeauQualiteNappesAPI(APIClient):
    """Scraper for Hub'Eau Qualité Nappes API v1."""

    BASE_URL = "https://hubeau.eaufrance.fr/api/v1/qualite_nappes"

    def __init__(
        self,
        task_id: str,
        state_manager: StateManager,
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs
    ):
        super().__init__(
            task_id=task_id,
            base_url=self.BASE_URL,
            state_manager=state_manager,
            rate_limiter=rate_limiter,
            **kwargs
        )

    def get_stations(
        self,
        bss_id: Optional[str] = None,
        code_commune: Optional[str] = None,
        bbox: Optional[str] = None,
    ) -> List[Dict]:
        """Get groundwater monitoring stations.

        Args:
            bss_id: BSS identifier (French groundwater database)
            code_commune: Municipality code
            bbox: Bounding box

        Returns:
            List of station records
        """
        params = {}
        if bss_id:
            params['bss_id'] = bss_id
        if code_commune:
            params['code_commune'] = code_commune
        if bbox:
            params['bbox'] = bbox

        return self._paginate_page_size(
            endpoint='/stations',
            params=params
        )

    def get_analyses(
        self,
        bss_id: Optional[str] = None,
        code_commune: Optional[str] = None,
        code_parametre: Optional[str] = None,
        date_debut_prelevement: Optional[str] = None,
        date_fin_prelevement: Optional[str] = None,
        nom_region: Optional[str] = None,
    ) -> List[Dict]:
        """Get groundwater quality analyses.

        Args:
            bss_id: BSS identifier
            code_commune: Municipality code
            code_parametre: Parameter code
            date_debut_prelevement: Start date (YYYY-MM-DD)
            date_fin_prelevement: End date (YYYY-MM-DD)
            nom_region: Region name (e.g., "Bretagne") - if provided, uses region filter instead of daily iteration

        Returns:
            List of analysis records
        """
        params = {}
        if bss_id:
            params['bss_id'] = bss_id
        if code_commune:
            params['code_commune'] = code_commune
        if code_parametre:
            params['code_parametre'] = code_parametre
        if date_debut_prelevement:
            params['date_debut_prelevement'] = date_debut_prelevement
        if date_fin_prelevement:
            params['date_fin_prelevement'] = date_fin_prelevement
        if nom_region:
            params['nom_region'] = nom_region

        return self._paginate_page_size(
            endpoint='/analyses',
            params=params
        )

    def run(self) -> None:
        """Not implemented - use specific methods instead."""
        raise NotImplementedError(
            "Use specific methods like get_analyses() instead"
        )


def export_to_parquet(data: List[Dict], output_path: Path) -> None:
    """Export data to Parquet format.

    Args:
        data: List of records
        output_path: Output file path
    """
    if not data:
        logger.warning("No data to export", output_path=str(output_path))
        return

    df = pd.DataFrame(data)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine='pyarrow')

    logger.info(
        "Data exported to Parquet",
        output_path=str(output_path),
        rows=len(df),
        columns=len(df.columns)
    )
