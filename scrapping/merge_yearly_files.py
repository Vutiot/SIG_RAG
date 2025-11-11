#!/usr/bin/env python3
"""Merge yearly parquet files by parameter."""

import glob
import pandas as pd
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)

def merge_files(pattern: str, output_path: Path):
    """Merge files matching pattern into output file."""
    files = sorted(glob.glob(pattern))

    if not files:
        logger.warning("No files found", pattern=pattern)
        return

    logger.info("Merging files", pattern=pattern, count=len(files))

    # Read and concatenate all files
    dfs = []
    total_records = 0

    for file_path in files:
        logger.info("Reading file", file=file_path)
        df = pd.read_parquet(file_path)
        total_records += len(df)
        dfs.append(df)
        logger.info("File loaded", records=len(df), total=total_records)

    logger.info("Concatenating dataframes", files=len(dfs))
    combined_df = pd.concat(dfs, ignore_index=True)

    logger.info("Writing combined file", output=str(output_path), records=len(combined_df))
    combined_df.to_parquet(output_path, index=False, engine='pyarrow')

    logger.info("Merge complete", output=str(output_path), records=len(combined_df))

    # Clear memory
    del dfs
    del combined_df

def main():
    base_path = Path("exports/parquet")

    # Merge NO3 files
    logger.info("===== Merging NO3 files =====")
    merge_files(
        "exports/parquet/qualite_nappes_NO3_*.parquet",
        base_path / "qualite_nappes_NO3_all.parquet"
    )

    # Merge TURB files
    logger.info("===== Merging TURB files =====")
    merge_files(
        "exports/parquet/qualite_nappes_TURB_*.parquet",
        base_path / "qualite_nappes_TURB_all.parquet"
    )

    logger.info("===== All merges complete =====")

if __name__ == "__main__":
    main()
