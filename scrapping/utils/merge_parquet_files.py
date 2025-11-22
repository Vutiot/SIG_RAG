#!/usr/bin/env python3
"""Merge NO3 and turbidity parquet files into combined files.

This script merges separate NO3 and TURB parquet files into single combined files
per year, containing both parameters together.
"""

import argparse
from pathlib import Path
from typing import List, Dict

import pandas as pd
import structlog

from utils.logger import setup_logger

logger = structlog.get_logger(__name__)


def find_file_pairs(parquet_dir: Path, pattern: str) -> Dict[str, Dict[str, Path]]:
    """Find pairs of NO3 and TURB files for the same year/region.

    Args:
        parquet_dir: Directory containing parquet files
        pattern: Base pattern to match (e.g., 'qualite_nappes_monthly')

    Returns:
        Dictionary mapping year/region to {'no3': path, 'turb': path}
    """
    no3_files = list(parquet_dir.glob(f"{pattern}_no3_*.parquet"))
    turb_files = list(parquet_dir.glob(f"{pattern}_turb_*.parquet"))

    pairs = {}

    for no3_file in no3_files:
        # Extract the year and region suffix from filename
        # e.g., qualite_nappes_monthly_no3_2024_04.parquet -> 2024_04
        parts = no3_file.stem.split('_no3_')
        if len(parts) == 2:
            suffix = parts[1]  # e.g., "2024_04"
            base = parts[0]    # e.g., "qualite_nappes_monthly"

            # Look for matching turb file
            turb_file = parquet_dir / f"{base}_turb_{suffix}.parquet"

            if turb_file.exists():
                pairs[suffix] = {
                    'no3': no3_file,
                    'turb': turb_file
                }
                logger.info(
                    "Found file pair",
                    suffix=suffix,
                    no3=no3_file.name,
                    turb=turb_file.name
                )
            else:
                logger.warning(
                    "NO3 file has no matching TURB file",
                    no3_file=no3_file.name,
                    expected_turb=turb_file.name
                )

    return pairs


def merge_file_pair(
    no3_path: Path,
    turb_path: Path,
    output_path: Path,
    dry_run: bool = False
) -> int:
    """Merge NO3 and TURB files into a single combined file.

    Args:
        no3_path: Path to NO3 parquet file
        turb_path: Path to TURB parquet file
        output_path: Path for merged output file
        dry_run: If True, don't actually write the file

    Returns:
        Total number of records in merged file
    """
    # Load both files
    df_no3 = pd.read_parquet(no3_path)
    df_turb = pd.read_parquet(turb_path)

    logger.info(
        "Loading files",
        no3_records=len(df_no3),
        turb_records=len(df_turb)
    )

    # Verify data integrity
    no3_params = df_no3['code_param'].unique()
    turb_params = df_turb['code_param'].unique()

    if not all(no3_params == 1340):
        logger.error(
            "NO3 file contains unexpected parameters",
            file=no3_path.name,
            params=no3_params.tolist()
        )
        raise ValueError(f"NO3 file {no3_path.name} contains non-NO3 data")

    if not all(turb_params == 1295):
        logger.error(
            "TURB file contains unexpected parameters",
            file=turb_path.name,
            params=turb_params.tolist()
        )
        raise ValueError(f"TURB file {turb_path.name} contains non-TURB data")

    # Merge dataframes
    df_merged = pd.concat([df_no3, df_turb], ignore_index=True)

    # Sort by date and parameter for consistency
    if 'date_debut_prelevement' in df_merged.columns:
        df_merged = df_merged.sort_values(['date_debut_prelevement', 'code_param'])

    total_records = len(df_merged)

    if dry_run:
        logger.info(
            "[DRY RUN] Would merge files",
            output=str(output_path),
            total_records=total_records,
            no3_records=len(df_no3),
            turb_records=len(df_turb)
        )
    else:
        # Save merged file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_merged.to_parquet(output_path, index=False, engine='pyarrow')

        file_size = output_path.stat().st_size / 1024  # KB

        logger.info(
            "Merged files saved",
            output=str(output_path),
            total_records=total_records,
            no3_records=len(df_no3),
            turb_records=len(df_turb),
            size_kb=f"{file_size:.1f}"
        )

    return total_records


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Merge NO3 and turbidity parquet files"
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=Path("exports/parquet"),
        help="Directory containing parquet files (default: exports/parquet)"
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="qualite_nappes_monthly",
        help="Base filename pattern to match (default: qualite_nappes_monthly)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for merged files (default: same as parquet-dir)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually merging files"
    )
    parser.add_argument(
        "--remove-originals",
        action="store_true",
        help="Remove original NO3 and TURB files after successful merge"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logger()

    parquet_dir = args.parquet_dir
    if not parquet_dir.exists():
        logger.error("Parquet directory does not exist", path=str(parquet_dir))
        return 1

    output_dir = args.output_dir or parquet_dir

    logger.info(
        "Starting merge",
        parquet_dir=str(parquet_dir),
        pattern=args.pattern,
        output_dir=str(output_dir),
        dry_run=args.dry_run
    )

    # Find file pairs
    pairs = find_file_pairs(parquet_dir, args.pattern)

    if not pairs:
        logger.warning("No file pairs found to merge")
        return 0

    logger.info(f"Found {len(pairs)} file pairs to merge")

    # Merge each pair
    total_merged = 0
    merged_files = []

    for suffix, paths in sorted(pairs.items()):
        # Build output filename
        # e.g., qualite_nappes_monthly_no3_turb_2024_04.parquet
        output_name = f"{args.pattern}_no3_turb_{suffix}.parquet"
        output_path = output_dir / output_name

        try:
            records = merge_file_pair(
                paths['no3'],
                paths['turb'],
                output_path,
                dry_run=args.dry_run
            )
            total_merged += records
            merged_files.append((paths, output_path))
        except Exception as e:
            logger.error(
                "Failed to merge files",
                suffix=suffix,
                error=str(e)
            )
            return 1

    logger.info(
        f"{'[DRY RUN] Would merge' if args.dry_run else 'Merged'} {len(pairs)} file pairs",
        total_records=total_merged
    )

    # Remove originals if requested
    if args.remove_originals and not args.dry_run:
        logger.info("Removing original files...")
        removed_count = 0

        for paths, output_path in merged_files:
            # Only remove if merge was successful
            if output_path.exists():
                paths['no3'].unlink()
                paths['turb'].unlink()
                removed_count += 2
                logger.info(
                    "Removed original files",
                    no3=paths['no3'].name,
                    turb=paths['turb'].name
                )

        logger.info(f"Removed {removed_count} original files")

    logger.info("Merge complete")
    return 0


if __name__ == "__main__":
    exit(main())
