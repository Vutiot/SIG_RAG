#!/usr/bin/env python3
"""Merge yearly parquet files into single files per parameter.

This script takes separate NO3 and TURB files for each year and merges them
into single files containing all years.
"""

import argparse
from pathlib import Path
from typing import List, Dict

import pandas as pd
import structlog

from utils.logger import setup_logger

logger = structlog.get_logger(__name__)


def split_combined_files(parquet_dir: Path, pattern: str) -> None:
    """Split combined no3_turb files back into separate no3 and turb files.

    Args:
        parquet_dir: Directory containing parquet files
        pattern: Base pattern to match (e.g., 'qualite_nappes_monthly')
    """
    combined_files = list(parquet_dir.glob(f"{pattern}_no3_turb_*.parquet"))

    if not combined_files:
        logger.info("No combined files to split")
        return

    logger.info(f"Found {len(combined_files)} combined files to split")

    for combined_file in combined_files:
        # Extract year and region from filename
        # e.g., qualite_nappes_monthly_no3_turb_2024_04.parquet
        parts = combined_file.stem.split('_no3_turb_')
        if len(parts) != 2:
            logger.warning("Unexpected filename format", file=combined_file.name)
            continue

        base = parts[0]  # qualite_nappes_monthly
        suffix = parts[1]  # 2024_04

        # Load combined file
        df = pd.read_parquet(combined_file)

        # Split by parameter
        df_no3 = df[df['code_param'] == 1340]
        df_turb = df[df['code_param'] == 1295]

        # Save separate files
        no3_path = parquet_dir / f"{base}_no3_{suffix}.parquet"
        turb_path = parquet_dir / f"{base}_turb_{suffix}.parquet"

        if len(df_no3) > 0:
            df_no3.to_parquet(no3_path, index=False, engine='pyarrow')
            logger.info("Created NO3 file", file=no3_path.name, records=len(df_no3))

        if len(df_turb) > 0:
            df_turb.to_parquet(turb_path, index=False, engine='pyarrow')
            logger.info("Created TURB file", file=turb_path.name, records=len(df_turb))

        # Remove combined file
        combined_file.unlink()
        logger.info("Removed combined file", file=combined_file.name)


def merge_years_by_parameter(
    parquet_dir: Path,
    pattern: str,
    param_name: str,
    output_path: Path
) -> int:
    """Merge all yearly files for a parameter into a single file.

    Args:
        parquet_dir: Directory containing parquet files
        pattern: Base pattern to match (e.g., 'qualite_nappes_monthly')
        param_name: Parameter name ('no3' or 'turb')
        output_path: Output path for merged file

    Returns:
        Total number of records
    """
    # Find all files for this parameter
    files = sorted(parquet_dir.glob(f"{pattern}_{param_name}_*.parquet"))

    if not files:
        logger.warning(f"No {param_name.upper()} files found to merge")
        return 0

    logger.info(f"Found {len(files)} {param_name.upper()} files to merge", files=len(files))

    # Load and concatenate all files
    dfs = []
    for file in files:
        df = pd.read_parquet(file)
        dfs.append(df)
        logger.info(f"Loaded {param_name.upper()} file", file=file.name, records=len(df))

    # Combine all dataframes
    df_merged = pd.concat(dfs, ignore_index=True)

    # Sort by date for consistency
    if 'date_debut_prelevement' in df_merged.columns:
        df_merged = df_merged.sort_values('date_debut_prelevement')

    # Save merged file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_merged.to_parquet(output_path, index=False, engine='pyarrow')

    file_size = output_path.stat().st_size / 1024  # KB

    logger.info(
        f"Merged {param_name.upper()} files saved",
        output=str(output_path),
        total_records=len(df_merged),
        files_merged=len(files),
        size_kb=f"{file_size:.1f}"
    )

    return len(df_merged)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Merge yearly parquet files by parameter"
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
        help="Base filename pattern (default: qualite_nappes_monthly)"
    )
    parser.add_argument(
        "--region-suffix",
        type=str,
        default="04",
        help="Region/basin suffix (default: 04 for Loire-Bretagne)"
    )
    parser.add_argument(
        "--remove-yearly",
        action="store_true",
        help="Remove individual yearly files after merging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logger()

    parquet_dir = args.parquet_dir
    if not parquet_dir.exists():
        logger.error("Parquet directory does not exist", path=str(parquet_dir))
        return 1

    logger.info(
        "Starting year merge",
        parquet_dir=str(parquet_dir),
        pattern=args.pattern,
        region=args.region_suffix
    )

    # Step 1: Split combined files if they exist
    logger.info("Step 1: Splitting combined no3_turb files...")
    split_combined_files(parquet_dir, args.pattern)

    # Step 2: Merge years for each parameter
    logger.info("Step 2: Merging years by parameter...")

    # Merge NO3 files
    no3_output = parquet_dir / f"{args.pattern}_no3_{args.region_suffix}.parquet"
    no3_records = merge_years_by_parameter(
        parquet_dir,
        args.pattern,
        "no3",
        no3_output
    )

    # Merge TURB files
    turb_output = parquet_dir / f"{args.pattern}_turb_{args.region_suffix}.parquet"
    turb_records = merge_years_by_parameter(
        parquet_dir,
        args.pattern,
        "turb",
        turb_output
    )

    logger.info(
        "Year merge complete",
        no3_records=no3_records,
        turb_records=turb_records,
        total_records=no3_records + turb_records
    )

    # Step 3: Remove yearly files if requested
    if args.remove_yearly:
        logger.info("Removing individual yearly files...")

        yearly_files = list(parquet_dir.glob(f"{args.pattern}_no3_*_*.parquet"))
        yearly_files.extend(parquet_dir.glob(f"{args.pattern}_turb_*_*.parquet"))

        removed_count = 0
        for file in yearly_files:
            # Don't remove the merged files
            if file not in [no3_output, turb_output]:
                file.unlink()
                removed_count += 1
                logger.info("Removed yearly file", file=file.name)

        logger.info(f"Removed {removed_count} yearly files")

    logger.info("All done!")
    return 0


if __name__ == "__main__":
    exit(main())
