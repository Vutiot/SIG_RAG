"""Spatial operations for geographic data processing.

Handles:
- Spatial joins (point-in-polygon, nearest neighbor)
- Snapping points to line networks
- Buffer operations
"""

from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import structlog

logger = structlog.get_logger(__name__)


class SpatialProcessor:
    """Process spatial data and perform geographic operations."""

    def __init__(self, task_id: str):
        """Initialize spatial processor.

        Args:
            task_id: Task identifier
        """
        self.task_id = task_id
        self.logger = logger.bind(task_id=task_id)

    def spatial_join_communes(
        self,
        data_file: Union[Path, pd.DataFrame],
        communes_file: Path,
        output_file: Path,
        how: str = 'left',
        predicate: str = 'intersects'
    ) -> gpd.GeoDataFrame:
        """Join data with commune boundaries.

        Args:
            data_file: Input data (Parquet or DataFrame) with geometry
            communes_file: GeoJSON file with commune boundaries
            output_file: Output Parquet file
            how: Type of join ('left', 'right', 'inner')
            predicate: Spatial predicate ('intersects', 'within', 'contains')

        Returns:
            Joined GeoDataFrame
        """
        self.logger.info(
            "Performing spatial join with communes",
            data=str(data_file) if isinstance(data_file, Path) else "DataFrame",
            communes=str(communes_file)
        )

        # Load data
        if isinstance(data_file, Path):
            if data_file.suffix == '.parquet':
                df = pd.read_parquet(data_file)
            else:
                df = gpd.read_file(data_file)
        else:
            df = data_file

        # Ensure it's a GeoDataFrame
        if not isinstance(df, gpd.GeoDataFrame):
            # Try to create geometries from lat/lon columns
            if 'latitude' in df.columns and 'longitude' in df.columns:
                geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
                df = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
            elif 'lat' in df.columns and 'lon' in df.columns:
                geometry = [Point(xy) for xy in zip(df.lon, df.lat)]
                df = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
            else:
                raise ValueError("Cannot create geometry from data")

        # Load communes
        communes = gpd.read_file(communes_file)

        # Ensure same CRS
        if df.crs != communes.crs:
            df = df.to_crs(communes.crs)

        # Perform spatial join
        joined = gpd.sjoin(df, communes, how=how, predicate=predicate)

        # Save to Parquet
        output_file.parent.mkdir(parents=True, exist_ok=True)
        joined.to_parquet(output_file, index=False)

        self.logger.info(
            "Spatial join complete",
            input_rows=len(df),
            output_rows=len(joined),
            output=str(output_file)
        )

        return joined

    def snap_to_network(
        self,
        data_file: Union[Path, pd.DataFrame],
        network_file: Path,
        output_file: Path,
        buffer_m: float = 100,
        method: str = 'nearest'
    ) -> gpd.GeoDataFrame:
        """Snap points to nearest line in a network.

        Args:
            data_file: Input data with point geometries
            network_file: GeoJSON file with line network (e.g., BD TOPAGE)
            output_file: Output Parquet file
            buffer_m: Buffer distance in meters for finding candidates
            method: Snapping method ('nearest')

        Returns:
            GeoDataFrame with snapped geometries and matched network IDs
        """
        self.logger.info(
            "Snapping points to network",
            data=str(data_file) if isinstance(data_file, Path) else "DataFrame",
            network=str(network_file),
            buffer=buffer_m
        )

        # Load data
        if isinstance(data_file, Path):
            if data_file.suffix == '.parquet':
                df = pd.read_parquet(data_file)
                if not isinstance(df, gpd.GeoDataFrame):
                    # Reconstruct geometry
                    from shapely import wkb
                    df['geometry'] = df['geometry'].apply(lambda x: wkb.loads(x) if isinstance(x, bytes) else x)
                    df = gpd.GeoDataFrame(df, geometry='geometry')
            else:
                df = gpd.read_file(data_file)
        else:
            df = data_file

        # Load network
        network = gpd.read_file(network_file)

        # Ensure same CRS (use projected CRS for buffer)
        target_crs = 'EPSG:2154'  # Lambert 93 (France)
        df = df.to_crs(target_crs)
        network = network.to_crs(target_crs)

        # Find nearest network segment for each point
        snapped_results = []

        for idx, point_row in df.iterrows():
            point_geom = point_row.geometry

            # Find candidate segments within buffer
            candidates = network[network.distance(point_geom) <= buffer_m]

            if len(candidates) > 0:
                # Find nearest
                distances = candidates.geometry.distance(point_geom)
                nearest_idx = distances.idxmin()
                nearest_segment = network.loc[nearest_idx]

                # Get nearest point on line
                snapped_point = nearest_segment.geometry.interpolate(
                    nearest_segment.geometry.project(point_geom)
                )

                result = point_row.to_dict()
                result['snapped_geometry'] = snapped_point
                result['network_id'] = nearest_segment.get('id_troncon', nearest_idx)
                result['snap_distance_m'] = point_geom.distance(snapped_point)
                result['original_geometry'] = point_geom

            else:
                # No network within buffer
                result = point_row.to_dict()
                result['snapped_geometry'] = None
                result['network_id'] = None
                result['snap_distance_m'] = None
                result['original_geometry'] = point_geom

            snapped_results.append(result)

        # Create GeoDataFrame with snapped geometries
        result_gdf = gpd.GeoDataFrame(
            snapped_results,
            geometry='snapped_geometry',
            crs=target_crs
        )

        # Convert back to WGS84 for storage
        result_gdf = result_gdf.to_crs('EPSG:4326')

        # Save to Parquet
        output_file.parent.mkdir(parents=True, exist_ok=True)
        result_gdf.to_parquet(output_file, index=False)

        successful_snaps = result_gdf['network_id'].notna().sum()
        self.logger.info(
            "Snapping complete",
            input_rows=len(df),
            successful_snaps=successful_snaps,
            failed_snaps=len(df) - successful_snaps,
            output=str(output_file)
        )

        return result_gdf

    def nearest_join(
        self,
        left_file: Union[Path, gpd.GeoDataFrame],
        right_file: Path,
        output_file: Path,
        max_distance_m: Optional[float] = None
    ) -> gpd.GeoDataFrame:
        """Join datasets based on nearest neighbor.

        Args:
            left_file: Left dataset (points or polygons)
            right_file: Right dataset
            output_file: Output file
            max_distance_m: Maximum distance in meters (None = no limit)

        Returns:
            Joined GeoDataFrame
        """
        self.logger.info(
            "Performing nearest neighbor join",
            left=str(left_file) if isinstance(left_file, Path) else "GeoDataFrame",
            right=str(right_file)
        )

        # Load data
        if isinstance(left_file, Path):
            if left_file.suffix == '.parquet':
                left_gdf = gpd.read_parquet(left_file)
            else:
                left_gdf = gpd.read_file(left_file)
        else:
            left_gdf = left_file

        right_gdf = gpd.read_file(right_file)

        # Ensure same CRS
        if left_gdf.crs != right_gdf.crs:
            right_gdf = right_gdf.to_crs(left_gdf.crs)

        # Use projected CRS for distance calculation
        target_crs = 'EPSG:2154'  # Lambert 93
        left_gdf = left_gdf.to_crs(target_crs)
        right_gdf = right_gdf.to_crs(target_crs)

        # Perform nearest join
        joined = gpd.sjoin_nearest(
            left_gdf,
            right_gdf,
            how='left',
            max_distance=max_distance_m,
            distance_col='distance_m'
        )

        # Convert back to WGS84
        joined = joined.to_crs('EPSG:4326')

        # Save
        output_file.parent.mkdir(parents=True, exist_ok=True)
        joined.to_parquet(output_file, index=False)

        self.logger.info(
            "Nearest join complete",
            input_rows=len(left_gdf),
            output_rows=len(joined),
            output=str(output_file)
        )

        return joined

    def buffer_analysis(
        self,
        points_file: Path,
        buffer_m: float,
        overlay_file: Path,
        output_file: Path
    ) -> gpd.GeoDataFrame:
        """Create buffers around points and overlay with another dataset.

        Args:
            points_file: File with point geometries
            buffer_m: Buffer radius in meters
            overlay_file: File to overlay
            output_file: Output file

        Returns:
            GeoDataFrame with buffer analysis results
        """
        self.logger.info(
            "Performing buffer analysis",
            points=str(points_file),
            buffer_m=buffer_m,
            overlay=str(overlay_file)
        )

        # Load points
        points = gpd.read_file(points_file)

        # Load overlay data
        overlay = gpd.read_file(overlay_file)

        # Use projected CRS for buffer
        target_crs = 'EPSG:2154'
        points = points.to_crs(target_crs)
        overlay = overlay.to_crs(target_crs)

        # Create buffers
        points['buffer_geometry'] = points.geometry.buffer(buffer_m)

        # Set buffer as active geometry
        buffered = points.set_geometry('buffer_geometry')

        # Overlay
        result = gpd.overlay(buffered, overlay, how='intersection')

        # Convert back
        result = result.to_crs('EPSG:4326')

        # Save
        output_file.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(output_file, index=False)

        self.logger.info(
            "Buffer analysis complete",
            input_points=len(points),
            output_features=len(result),
            output=str(output_file)
        )

        return result
