#!/usr/bin/env python3
"""Main orchestrator for Loire-Bretagne scraping system.

Executes all tasks defined in the playbook:
- t1-t3: Download geographic data
- t4-t6: Harvest API data
- t7-t8: Crawl and download PDFs
- t9-t10: Process PDFs with OCR
- pp1-pp3: Post-processing (spatial joins, graph building)
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import structlog

from utils.logger import setup_logger, get_logger, MetricsLogger
from utils.rate_limiter import RateLimiter, set_rate_limiter
from utils.state_manager import StateManager
from scrapers.api_scrapers import (
    HubeauQualiteRivieresAPI,
    HubeauHydrometrieAPI,
    HubeauQualiteNappesAPI,
    export_to_parquet
)
from scrapers.geo_downloaders import GeoDownloader
from scrapers.web_spiders import run_spider, BSHSpider, AELBSpider
from processing.pdf_processor import process_pdfs_batch
from processing.spatial_ops import SpatialProcessor
from processing.graph_builder import GraphBuilder

logger = structlog.get_logger(__name__)


class TaskOrchestrator:
    """Orchestrate all scraping and processing tasks."""

    def __init__(
        self,
        playbook_path: Path,
        skip_completed: bool = True,
        tasks_to_run: Optional[List[str]] = None
    ):
        """Initialize orchestrator.

        Args:
            playbook_path: Path to playbook JSON
            skip_completed: Skip already completed tasks
            tasks_to_run: List of specific task IDs to run (None = all)
        """
        self.playbook_path = playbook_path
        self.skip_completed = skip_completed
        self.tasks_to_run = tasks_to_run

        # Load playbook
        with open(playbook_path, 'r', encoding='utf-8') as f:
            self.playbook = json.load(f)

        # Initialize utilities
        self.state_manager = StateManager()
        self.rate_limiter = RateLimiter.from_playbook(self.playbook)
        set_rate_limiter(self.rate_limiter)

        self.logger = get_logger(__name__)
        self.logger.info(
            "Orchestrator initialized",
            playbook=str(playbook_path),
            tasks=len(self.playbook.get('tasks', []))
        )

    def should_run_task(self, task_id: str) -> bool:
        """Check if task should be run.

        Args:
            task_id: Task identifier

        Returns:
            True if task should run, False otherwise
        """
        # Check if specific tasks were requested
        if self.tasks_to_run and task_id not in self.tasks_to_run:
            return False

        # Check if already completed
        if self.skip_completed and self.state_manager.is_task_completed(task_id):
            self.logger.info("Task already completed, skipping", task_id=task_id)
            return False

        return True

    def run_task_t1(self, task: Dict) -> None:
        """Task t1: Download Admin Express communes."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Download Admin Express")
        self.state_manager.start_task(task_id)

        try:
            downloader = GeoDownloader(
                task_id=task_id,
                output_dir=Path("processed/geo"),
                state_manager=self.state_manager,
                rate_limiter=self.rate_limiter
            )

            # Find Admin Express source in playbook
            source = next(s for s in self.playbook['sources'] if s['id'] == 'admin_express_communes')
            url = source['resources'][0]['url']

            output_path = Path(task['output'])
            downloader.download_and_extract_admin_express(url, output_path)

            self.state_manager.complete_task(task_id)
            self.logger.info("Task completed", task_id=task_id)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t2(self, task: Dict) -> None:
        """Task t2: Download BD TOPAGE."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Download BD TOPAGE")
        self.state_manager.start_task(task_id)

        try:
            downloader = GeoDownloader(
                task_id=task_id,
                output_dir=Path("processed/geo"),
                state_manager=self.state_manager,
                rate_limiter=self.rate_limiter
            )

            source = next(s for s in self.playbook['sources'] if s['id'] == 'bd_topage_troncons')
            url = source['resources'][0]['url']

            output_path = Path(task['output'])
            downloader.download_and_extract_topage(url, output_path)

            self.state_manager.complete_task(task_id)
            self.logger.info("Task completed", task_id=task_id)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t3(self, task: Dict) -> None:
        """Task t3: Download ROE obstacles."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Download ROE")
        self.state_manager.start_task(task_id)

        try:
            downloader = GeoDownloader(
                task_id=task_id,
                output_dir=Path("processed/geo"),
                state_manager=self.state_manager,
                rate_limiter=self.rate_limiter
            )

            source = next(s for s in self.playbook['sources'] if s['id'] == 'roe_obstacles')
            url = source['resources'][0]['url']

            output_path = Path(task['output'])
            downloader.download_and_extract_roe(url, output_path)

            self.state_manager.complete_task(task_id)
            self.logger.info("Task completed", task_id=task_id)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t4(self, task: Dict) -> None:
        """Task t4: Harvest Hub'Eau quality data."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Harvest quality rivers data")
        self.state_manager.start_task(task_id)

        try:
            api = HubeauQualiteRivieresAPI(
                task_id=task_id,
                state_manager=self.state_manager,
                rate_limiter=self.rate_limiter
            )

            params = task['params']
            all_results = []

            # Iterate over parameters and periods
            for param_code in params['code_parametre']:
                for period in params['periods']:
                    date_min, date_max = period.split('/')

                    self.logger.info(
                        "Fetching analyses",
                        parameter=param_code,
                        period=period
                    )

                    results = api.get_analyses_pc(
                        code_parametre=param_code,
                        date_min_prelevement=date_min,
                        date_max_prelevement=date_max
                    )

                    all_results.extend(results)

            # Export to Parquet
            output_path = Path(task['output'])
            export_to_parquet(all_results, output_path)

            self.state_manager.complete_task(task_id, {'records': len(all_results)})
            self.logger.info("Task completed", task_id=task_id, records=len(all_results))

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t5(self, task: Dict) -> None:
        """Task t5: Harvest hydrometry data."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Harvest hydrometry data")
        self.state_manager.start_task(task_id)

        try:
            api = HubeauHydrometrieAPI(
                task_id=task_id,
                state_manager=self.state_manager,
                rate_limiter=self.rate_limiter
            )

            params = task['params']
            all_results = []

            for grandeur in params['grandeur_hydro']:
                for period in params['periods']:
                    date_min, date_max = period.split('/')

                    self.logger.info(
                        "Fetching observations",
                        grandeur=grandeur,
                        period=period
                    )

                    results = api.get_obs_elab(
                        grandeur_hydro=grandeur,
                        date_debut_obs=date_min,
                        date_fin_obs=date_max
                    )

                    all_results.extend(results)

            output_path = Path(task['output'])
            export_to_parquet(all_results, output_path)

            self.state_manager.complete_task(task_id, {'records': len(all_results)})
            self.logger.info("Task completed", task_id=task_id, records=len(all_results))

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t6(self, task: Dict) -> None:
        """Task t6: Harvest groundwater quality data.

        Supports two modes:
        1. Daily iteration (default): Queries API day-by-day to avoid pagination limits
        2. Region filter: Uses nom_region parameter for faster regional queries

        Mode is automatically selected based on presence of 'nom_region' in params.
        """
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Harvest groundwater quality")
        self.state_manager.start_task(task_id)

        try:
            api = HubeauQualiteNappesAPI(
                task_id=task_id,
                state_manager=self.state_manager,
                rate_limiter=self.rate_limiter
            )

            params = task['params']
            output_base_path = Path(task['output'])
            yearly_files = []
            total_records = 0

            # Check if region filter is specified
            nom_region = params.get('nom_region', None)

            if nom_region:
                # MODE 1: Region-based approach (faster, uses yearly queries)
                self.logger.info("Using region filter mode", region=nom_region)

                for param_code in params['code_parametre']:
                    for period in params['periods']:
                        date_min, date_max = period.split('/')
                        start_date = datetime.strptime(date_min, '%Y-%m-%d')
                        end_date = datetime.strptime(date_max, '%Y-%m-%d')

                        # Process year by year with region filter
                        for year in range(start_date.year, end_date.year + 1):
                            year_start = datetime(year, 1, 1)
                            year_end = datetime(year, 12, 31)

                            if year_start < start_date:
                                year_start = start_date
                            if year_end > end_date:
                                year_end = end_date

                            self.logger.info(
                                "Fetching groundwater analyses",
                                parameter=param_code,
                                year=year,
                                start=year_start.strftime('%Y-%m-%d'),
                                end=year_end.strftime('%Y-%m-%d'),
                                region=nom_region
                            )

                            # Fetch data with region filter (API handles pagination)
                            results = api.get_analyses(
                                code_parametre=param_code,
                                date_debut_prelevement=year_start.strftime('%Y-%m-%d'),
                                date_fin_prelevement=year_end.strftime('%Y-%m-%d'),
                                nom_region=nom_region
                            )

                            if results:
                                region_suffix = f"_{nom_region}"
                                yearly_output_path = output_base_path.parent / f"{output_base_path.stem}_{param_code}_{year}{region_suffix}.parquet"
                                export_to_parquet(results, yearly_output_path)
                                yearly_files.append(yearly_output_path)
                                total_records += len(results)

                                self.logger.info(
                                    "Yearly data saved",
                                    parameter=param_code,
                                    year=year,
                                    records=len(results),
                                    output=str(yearly_output_path)
                                )
            else:
                # MODE 2: Daily iteration approach (slower, avoids pagination limits)
                self.logger.info("Using daily iteration mode (no region filter)")

                for param_code in params['code_parametre']:
                    for period in params['periods']:
                        date_min, date_max = period.split('/')
                        start_date = datetime.strptime(date_min, '%Y-%m-%d')
                        end_date = datetime.strptime(date_max, '%Y-%m-%d')

                        # Process year by year
                        for year in range(start_date.year, end_date.year + 1):
                            year_start = datetime(year, 1, 1)
                            year_end = datetime(year, 12, 31)

                            if year_start < start_date:
                                year_start = start_date
                            if year_end > end_date:
                                year_end = end_date

                            self.logger.info(
                                "Processing year",
                                parameter=param_code,
                                year=year,
                                start=year_start.strftime('%Y-%m-%d'),
                                end=year_end.strftime('%Y-%m-%d')
                            )

                            year_results = []

                            # Generate daily date ranges for this year
                            current_date = year_start
                            while current_date <= year_end:
                                day_str = current_date.strftime('%Y-%m-%d')

                                self.logger.info(
                                    "Fetching groundwater analyses",
                                    parameter=param_code,
                                    date=day_str
                                )

                                results = api.get_analyses(
                                    code_parametre=param_code,
                                    date_debut_prelevement=day_str,
                                    date_fin_prelevement=day_str
                                )

                                year_results.extend(results)

                                # Small delay between daily queries
                                time.sleep(0.2)

                                # Move to next day
                                current_date = current_date + timedelta(days=1)

                            # Save yearly file
                            if year_results:
                                yearly_output_path = output_base_path.parent / f"{output_base_path.stem}_{param_code}_{year}.parquet"
                                export_to_parquet(year_results, yearly_output_path)
                                yearly_files.append(yearly_output_path)
                                total_records += len(year_results)

                                self.logger.info(
                                    "Yearly data saved",
                                    parameter=param_code,
                                    year=year,
                                    records=len(year_results),
                                    output=str(yearly_output_path)
                                )

                                # Clear memory
                                year_results = []

            self.state_manager.complete_task(task_id, {'records': total_records})
            self.logger.info("Task completed", task_id=task_id, records=total_records, mode="region_filter" if nom_region else "daily_iteration")

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t7(self, task: Dict) -> None:
        """Task t7: Crawl BSH PDFs."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Crawl BSH PDFs")
        self.state_manager.start_task(task_id)

        try:
            run_spider(BSHSpider, task['output'], self.state_manager)

            self.state_manager.complete_task(task_id)
            self.logger.info("Task completed", task_id=task_id)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t8(self, task: Dict) -> None:
        """Task t8: Crawl AELB PDFs."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Crawl AELB PDFs")
        self.state_manager.start_task(task_id)

        try:
            run_spider(AELBSpider, task['output'], self.state_manager)

            self.state_manager.complete_task(task_id)
            self.logger.info("Task completed", task_id=task_id)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t9(self, task: Dict) -> None:
        """Task t9: Process AELB PDFs with OCR."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Process AELB PDFs")
        self.state_manager.start_task(task_id)

        try:
            results = process_pdfs_batch(
                input_dir=Path(task['input']),
                output_dir=Path(task['output']),
                task_id=task_id,
                state_manager=self.state_manager,
                use_ocr=True
            )

            successful = len([r for r in results if 'error' not in r])
            self.state_manager.complete_task(task_id, {'processed': successful})
            self.logger.info("Task completed", task_id=task_id, processed=successful)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t10(self, task: Dict) -> None:
        """Task t10: Process BSH PDFs with OCR."""
        task_id = task['id']
        if not self.should_run_task(task_id):
            return

        self.logger.info("Running task", task_id=task_id, description="Process BSH PDFs")
        self.state_manager.start_task(task_id)

        try:
            results = process_pdfs_batch(
                input_dir=Path(task['input']),
                output_dir=Path(task['output']),
                task_id=task_id,
                state_manager=self.state_manager,
                use_ocr=True
            )

            successful = len([r for r in results if 'error' not in r])
            self.state_manager.complete_task(task_id, {'processed': successful})
            self.logger.info("Task completed", task_id=task_id, processed=successful)

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_post_processing(self) -> None:
        """Run post-processing tasks."""
        post_tasks = self.playbook.get('post_processing', [])

        for task in post_tasks:
            task_id = task['id']

            if not self.should_run_task(task_id):
                continue

            self.logger.info("Running post-processing", task_id=task_id)
            self.state_manager.start_task(task_id)

            try:
                if task['action'] == 'spatial_join':
                    processor = SpatialProcessor(task_id)
                    processor.nearest_join(
                        left_file=Path(task['inputs'][0]),
                        right_file=Path(task['inputs'][1]),
                        output_file=Path(task['output'])
                    )

                elif task['action'] == 'snap_to_troncon':
                    processor = SpatialProcessor(task_id)
                    processor.snap_to_network(
                        data_file=Path(task['inputs'][0]),
                        network_file=Path(task['inputs'][1]),
                        output_file=Path(task['output']),
                        buffer_m=task['buffer_m']
                    )

                elif task['action'] == 'build_upstream_graph':
                    builder = GraphBuilder(task_id)
                    builder.build_hydro_graph(
                        troncons_file=Path(task['inputs'][0]),
                        output_file=Path(task['output'])
                    )

                self.state_manager.complete_task(task_id)
                self.logger.info("Post-processing completed", task_id=task_id)

            except Exception as e:
                self.logger.error("Post-processing failed", task_id=task_id, error=str(e))
                raise

    def run_all(self) -> None:
        """Run all tasks in order."""
        self.logger.info("Starting orchestrator")

        # Main tasks
        task_methods = {
            't1': self.run_task_t1,
            't2': self.run_task_t2,
            't3': self.run_task_t3,
            't4': self.run_task_t4,
            't5': self.run_task_t5,
            't6': self.run_task_t6,
            't7': self.run_task_t7,
            't8': self.run_task_t8,
            't9': self.run_task_t9,
            't10': self.run_task_t10,
        }

        for task in self.playbook['tasks']:
            task_id = task['id']
            if task_id in task_methods:
                task_methods[task_id](task)

        # Post-processing
        self.run_post_processing()

        self.logger.info("All tasks completed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Loire-Bretagne RAG Data Scraper")
    parser.add_argument(
        '--playbook',
        type=Path,
        default=Path('agent_scrape_playbook_loire_bretagne.json'),
        help='Path to playbook JSON file'
    )
    parser.add_argument(
        '--tasks',
        nargs='+',
        help='Specific task IDs to run (e.g., t1 t2 t3)'
    )
    parser.add_argument(
        '--no-skip-completed',
        action='store_true',
        help='Re-run already completed tasks'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logger(level=args.log_level)

    # Run orchestrator
    try:
        orchestrator = TaskOrchestrator(
            playbook_path=args.playbook,
            skip_completed=not args.no_skip_completed,
            tasks_to_run=args.tasks
        )
        orchestrator.run_all()
        print("\n✓ All tasks completed successfully!")
        return 0

    except KeyboardInterrupt:
        print("\n✗ Interrupted by user")
        return 130

    except Exception as e:
        print(f"\n✗ Error: {e}")
        logger.exception("Fatal error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
