#!/usr/bin/env python3
"""Cleanup script to remove old unfiltered parquet files.

This script identifies and removes parquet files that contain unfiltered data
(all ~1000 parameters) before the system was changed to filter and save only
NO3 and turbidity data.

Old file patterns to remove:
- qualite_nappes_*_1340_*.parquet (NO3 code, but contains all parameters)
- qualite_nappes_*_1295_*.parquet (TURB code, but contains all parameters)
- qualite_nappes_*_NO3_*.parquet (NO3 name, but contains all parameters)
- qualite_nappes_*_TURB_*.parquet (TURB name, but contains all parameters)

New file patterns to keep:
- qualite_nappes_*_no3_*.parquet (filtered NO3 only)
- qualite_nappes_*_turb_*.parquet (filtered turbidity only)
"""

import argparse
import shutil
from datetime import datetime
from pathlib import Path
from typing import List

import structlog

from utils.logger import setup_logger

logger = structlog.get_logger(__name__)


def identify_old_files(parquet_dir: Path) -> List[Path]:
    """Identify old unfiltered parquet files.

    Args:
        parquet_dir: Directory containing parquet files

    Returns:
        List of old files to remove
    """
    old_patterns = [
        "*_1340_*.parquet",  # Numeric NO3 code
        "*_1295_*.parquet",  # Numeric TURB code
        "*_NO3_*.parquet",   # Uppercase NO3
        "*_TURB_*.parquet",  # Uppercase TURB
    ]

    old_files = []
    for pattern in old_patterns:
        files = list(parquet_dir.glob(pattern))
        old_files.extend(files)

    # Remove duplicates
    old_files = list(set(old_files))

    # Filter out any files with lowercase parameter names (these are new filtered files)
    old_files = [
        f for f in old_files
        if not ('_no3_' in f.name or '_turb_' in f.name)
    ]

    return sorted(old_files)


def verify_new_files_exist(parquet_dir: Path) -> bool:
    """Verify that new filtered files exist before removing old ones.

    Args:
        parquet_dir: Directory containing parquet files

    Returns:
        True if new filtered files exist
    """
    new_patterns = [
        "*_no3_*.parquet",   # Lowercase no3 (filtered)
        "*_turb_*.parquet",  # Lowercase turb (filtered)
    ]

    new_files = []
    for pattern in new_patterns:
        files = list(parquet_dir.glob(pattern))
        new_files.extend(files)

    return len(new_files) > 0


def create_backup(old_files: List[Path], backup_dir: Path, dry_run: bool = False) -> int:
    """Move old files to backup directory.

    Args:
        old_files: List of files to backup
        backup_dir: Backup directory path
        dry_run: If True, only log what would be done

    Returns:
        Number of files backed up
    """
    if not old_files:
        logger.info("No old files to backup")
        return 0

    if not dry_run:
        backup_dir.mkdir(parents=True, exist_ok=True)

    backed_up_count = 0
    total_size = 0

    for old_file in old_files:
        file_size = old_file.stat().st_size
        total_size += file_size

        if dry_run:
            logger.info(
                "[DRY RUN] Would move file to backup",
                file=old_file.name,
                size_mb=f"{file_size / 1024**2:.2f}"
            )
        else:
            backup_path = backup_dir / old_file.name
            shutil.move(str(old_file), str(backup_path))
            logger.info(
                "Moved file to backup",
                file=old_file.name,
                backup=str(backup_path),
                size_mb=f"{file_size / 1024**2:.2f}"
            )

        backed_up_count += 1

    logger.info(
        f"{'[DRY RUN] Would backup' if dry_run else 'Backed up'} {backed_up_count} files",
        total_size_mb=f"{total_size / 1024**2:.2f}",
        backup_dir=str(backup_dir) if not dry_run else "N/A"
    )

    return backed_up_count


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Cleanup old unfiltered parquet files"
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=Path("exports/parquet"),
        help="Directory containing parquet files (default: exports/parquet)"
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help="Backup directory (default: exports/parquet_backup_YYYYMMDD_HHMMSS)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually moving files"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logger()

    parquet_dir = args.parquet_dir
    if not parquet_dir.exists():
        logger.error("Parquet directory does not exist", path=str(parquet_dir))
        return 1

    # Set default backup directory with timestamp
    if args.backup_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = parquet_dir.parent / f"parquet_backup_{timestamp}"
    else:
        backup_dir = args.backup_dir

    logger.info("Starting cleanup", parquet_dir=str(parquet_dir), dry_run=args.dry_run)

    # Check if new filtered files exist
    if not verify_new_files_exist(parquet_dir):
        logger.warning(
            "No new filtered files found! "
            "Have you run the updated scraper yet? "
            "Aborting cleanup to avoid data loss."
        )
        return 1

    logger.info("New filtered files exist, proceeding with cleanup")

    # Identify old files
    old_files = identify_old_files(parquet_dir)

    if not old_files:
        logger.info("No old files to clean up")
        return 0

    logger.info(f"Found {len(old_files)} old unfiltered files to backup")

    # Show sample of files
    for i, f in enumerate(old_files[:5]):
        logger.info(f"  [{i+1}] {f.name}")
    if len(old_files) > 5:
        logger.info(f"  ... and {len(old_files) - 5} more")

    # Confirmation prompt (skip in dry-run or if --force)
    if not args.dry_run and not args.force:
        response = input(f"\nMove {len(old_files)} files to {backup_dir}? [y/N]: ")
        if response.lower() != 'y':
            logger.info("Cleanup cancelled by user")
            return 0

    # Backup old files
    backed_up = create_backup(old_files, backup_dir, dry_run=args.dry_run)

    logger.info(
        "Cleanup complete",
        backed_up=backed_up,
        backup_dir=str(backup_dir) if not args.dry_run else "N/A"
    )

    return 0


if __name__ == "__main__":
    exit(main())
