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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import structlog

from utils.logger import setup_logger, get_logger, MetricsLogger
from utils.rate_limiter import RateLimiter, set_rate_limiter
from utils.state_manager import StateManager
from utils.date_utils import (
    parse_period,
    generate_date_ranges,
    get_iteration_mode_from_params,
    format_date_for_api
)
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

        # Generate unique run ID (ISO timestamp)
        self.run_id = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        self.run_start_time = time.time()

        # Load playbook
        with open(playbook_path, 'r', encoding='utf-8') as f:
            self.playbook = json.load(f)

        # Initialize utilities
        self.state_manager = StateManager()
        self.rate_limiter = RateLimiter.from_playbook(self.playbook)
        set_rate_limiter(self.rate_limiter)

        self.logger = get_logger(__name__, run_id=self.run_id)
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
        """Task t4: Harvest river quality data from Hub'Eau.

        Supports three iteration modes via 'iteration_mode' parameter:
        - 'yearly': Fast but may hit pagination limits
        - 'monthly': Balanced approach (~12 API calls/year) - RECOMMENDED
        - 'daily': Slowest but avoids all pagination limits

        If not specified, defaults to 'monthly'.
        """
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
            output_base_path = Path(task['output'])
            total_records = 0

            # Get iteration mode (defaults to monthly)
            iteration_mode = get_iteration_mode_from_params(params)

            self.logger.info(
                "Starting harvest",
                iteration_mode=iteration_mode
            )

            # Determine sleep delay between API calls based on granularity
            sleep_delays = {'yearly': 0.5, 'monthly': 0.3, 'daily': 0.2}
            sleep_delay = sleep_delays.get(iteration_mode, 0.3)

            # Iterate over parameters and periods
            for param_code in params['code_parametre']:
                for period in params['periods']:
                    start_date, end_date = parse_period(period)

                    # Generate date ranges based on iteration mode
                    date_ranges = generate_date_ranges(start_date, end_date, iteration_mode)

                    self.logger.info(
                        "Processing parameter",
                        parameter=param_code,
                        period=period,
                        iteration_mode=iteration_mode,
                        total_ranges=len(date_ranges)
                    )

                    # Process year by year for file organization
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
                            start=format_date_for_api(year_start),
                            end=format_date_for_api(year_end)
                        )

                        year_results = []

                        # Filter date ranges that fall within this year
                        year_date_ranges = [
                            (range_start, range_end) for range_start, range_end in date_ranges
                            if range_start.year == year or range_end.year == year
                        ]

                        # Fetch data for each date range within the year
                        for range_start, range_end in year_date_ranges:
                            # Clip range to year boundaries
                            if range_start < year_start:
                                range_start = year_start
                            if range_end > year_end:
                                range_end = year_end

                            # Skip if range is outside year
                            if range_start > year_end or range_end < year_start:
                                continue

                            self.logger.info(
                                "Fetching river analyses",
                                parameter=param_code,
                                date_start=format_date_for_api(range_start),
                                date_end=format_date_for_api(range_end)
                            )

                            results = api.get_analyses_pc(
                                code_parametre=param_code,
                                date_min_prelevement=format_date_for_api(range_start),
                                date_max_prelevement=format_date_for_api(range_end)
                            )

                            year_results.extend(results)

                            # Delay between API calls
                            time.sleep(sleep_delay)

                        # Save yearly file
                        if year_results:
                            yearly_output_path = output_base_path.parent / f"{output_base_path.stem}_{param_code}_{year}.parquet"
                            export_to_parquet(year_results, yearly_output_path)
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
            self.logger.info(
                "Task completed",
                task_id=task_id,
                records=total_records,
                iteration_mode=iteration_mode
            )

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t5(self, task: Dict) -> None:
        """Task t5: Harvest hydrometry data from Hub'Eau.

        Supports three iteration modes via 'iteration_mode' parameter:
        - 'yearly': Fast but may hit pagination limits
        - 'monthly': Balanced approach (~12 API calls/year) - RECOMMENDED
        - 'daily': Slowest but avoids all pagination limits

        If not specified, defaults to 'monthly'.
        """
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
            output_base_path = Path(task['output'])
            total_records = 0

            # Get iteration mode (defaults to monthly)
            iteration_mode = get_iteration_mode_from_params(params)

            self.logger.info(
                "Starting harvest",
                iteration_mode=iteration_mode
            )

            # Determine sleep delay between API calls based on granularity
            sleep_delays = {'yearly': 0.5, 'monthly': 0.3, 'daily': 0.2}
            sleep_delay = sleep_delays.get(iteration_mode, 0.3)

            for grandeur in params['grandeur_hydro']:
                for period in params['periods']:
                    start_date, end_date = parse_period(period)

                    # Generate date ranges based on iteration mode
                    date_ranges = generate_date_ranges(start_date, end_date, iteration_mode)

                    self.logger.info(
                        "Processing hydrometric magnitude",
                        grandeur=grandeur,
                        period=period,
                        iteration_mode=iteration_mode,
                        total_ranges=len(date_ranges)
                    )

                    # Process year by year for file organization
                    for year in range(start_date.year, end_date.year + 1):
                        year_start = datetime(year, 1, 1)
                        year_end = datetime(year, 12, 31)

                        if year_start < start_date:
                            year_start = start_date
                        if year_end > end_date:
                            year_end = end_date

                        self.logger.info(
                            "Processing year",
                            grandeur=grandeur,
                            year=year,
                            start=format_date_for_api(year_start),
                            end=format_date_for_api(year_end)
                        )

                        year_results = []

                        # Filter date ranges that fall within this year
                        year_date_ranges = [
                            (range_start, range_end) for range_start, range_end in date_ranges
                            if range_start.year == year or range_end.year == year
                        ]

                        # Fetch data for each date range within the year
                        for range_start, range_end in year_date_ranges:
                            # Clip range to year boundaries
                            if range_start < year_start:
                                range_start = year_start
                            if range_end > year_end:
                                range_end = year_end

                            # Skip if range is outside year
                            if range_start > year_end or range_end < year_start:
                                continue

                            self.logger.info(
                                "Fetching hydrometry observations",
                                grandeur=grandeur,
                                date_start=format_date_for_api(range_start),
                                date_end=format_date_for_api(range_end)
                            )

                            results = api.get_obs_elab(
                                grandeur_hydro=grandeur,
                                date_debut_obs=format_date_for_api(range_start),
                                date_fin_obs=format_date_for_api(range_end)
                            )

                            year_results.extend(results)

                            # Delay between API calls
                            time.sleep(sleep_delay)

                        # Save yearly file
                        if year_results:
                            yearly_output_path = output_base_path.parent / f"{output_base_path.stem}_{grandeur}_{year}.parquet"
                            export_to_parquet(year_results, yearly_output_path)
                            total_records += len(year_results)

                            self.logger.info(
                                "Yearly data saved",
                                grandeur=grandeur,
                                year=year,
                                records=len(year_results),
                                output=str(yearly_output_path)
                            )

                            # Clear memory
                            year_results = []

            self.state_manager.complete_task(task_id, {'records': total_records})
            self.logger.info(
                "Task completed",
                task_id=task_id,
                records=total_records,
                iteration_mode=iteration_mode
            )

        except Exception as e:
            self.logger.error("Task failed", task_id=task_id, error=str(e))
            raise

    def run_task_t6(self, task: Dict) -> None:
        """Task t6: Harvest groundwater quality data.

        Supports three iteration modes via 'iteration_mode' parameter:
        - 'yearly': Fast but may hit pagination limits (default when nom_region specified)
        - 'monthly': Balanced approach (~12 API calls/year) - RECOMMENDED
        - 'daily': Slowest but avoids all pagination limits

        Region filter (nom_region) works across all modes.
        Backward compatible with 'use_daily_iteration' flag.
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
            total_records = 0

            # Get iteration mode and optional region/basin filter
            iteration_mode = get_iteration_mode_from_params(params)
            nom_region = params.get('nom_region', None)
            nom_bassin = params.get('nom_circonscription_administrative_bassin', None)
            code_bassin = params.get('code_circonscription_administrative_bassin', None)

            # Use basin if specified, otherwise use region
            filter_name = code_bassin or nom_bassin or nom_region
            filter_type = "basin" if (code_bassin or nom_bassin) else ("region" if nom_region else None)

            region_msg = f" with {filter_type} filter ({filter_name})" if filter_name else ""
            self.logger.info(
                "Starting harvest",
                iteration_mode=iteration_mode,
                region=filter_name if filter_name else "all"
            )

            # Determine sleep delay between API calls based on granularity
            sleep_delays = {'yearly': 0.5, 'monthly': 0.3, 'daily': 0.2}
            sleep_delay = sleep_delays.get(iteration_mode, 0.3)

            # Get parameters to save (for filtering after fetch)
            parameters_to_save = params.get('parameters_to_save', [])
            if not parameters_to_save:
                raise ValueError("parameters_to_save must be specified in playbook params")

            for period in params['periods']:
                start_date, end_date = parse_period(period)

                # Generate date ranges based on iteration mode
                date_ranges = generate_date_ranges(start_date, end_date, iteration_mode)

                self.logger.info(
                    "Processing period (all parameters)",
                    period=period,
                    iteration_mode=iteration_mode,
                    total_ranges=len(date_ranges),
                    parameters=[f"{p['name']}({p['code']})" for p in parameters_to_save]
                )

                # Process year by year for file organization
                for year in range(start_date.year, end_date.year + 1):
                    year_start = datetime(year, 1, 1)
                    year_end = datetime(year, 12, 31)

                    if year_start < start_date:
                        year_start = start_date
                    if year_end > end_date:
                        year_end = end_date

                    log_msg = {
                        "year": year,
                        "start": format_date_for_api(year_start),
                        "end": format_date_for_api(year_end)
                    }
                    if filter_name:
                        log_msg[filter_type] = filter_name

                    self.logger.info("Processing year", **log_msg)

                    year_results = []

                    # Filter date ranges that fall within this year
                    year_date_ranges = [
                        (range_start, range_end) for range_start, range_end in date_ranges
                        if range_start.year == year or range_end.year == year
                    ]

                    # Fetch data for each date range within the year
                    for range_start, range_end in year_date_ranges:
                        # Clip range to year boundaries
                        if range_start < year_start:
                            range_start = year_start
                        if range_end > year_end:
                            range_end = year_end

                        # Skip if range is outside year
                        if range_start > year_end or range_end < year_start:
                            continue

                        log_msg = {
                            "date_start": format_date_for_api(range_start),
                            "date_end": format_date_for_api(range_end)
                        }
                        if filter_name:
                            log_msg[filter_type] = filter_name

                        self.logger.info("Fetching groundwater analyses (all parameters)", **log_msg)

                        # Build API call parameters (NO code_parametre filter - API ignores it anyway)
                        api_params = {
                            "date_debut_prelevement": format_date_for_api(range_start),
                            "date_fin_prelevement": format_date_for_api(range_end)
                        }
                        if nom_region:
                            api_params["nom_region"] = nom_region
                        elif code_bassin:
                            api_params["code_circonscription_administrative_bassin"] = code_bassin
                        elif nom_bassin:
                            api_params["nom_circonscription_administrative_bassin"] = nom_bassin

                        results = api.get_analyses(**api_params)
                        year_results.extend(results)

                        # Delay between API calls
                        time.sleep(sleep_delay)

                    # After fetching all data for the year, filter and save separate files per parameter
                    if year_results:
                        year_total = len(year_results)
                        region_suffix = f"_{filter_name}" if filter_name else ""

                        self.logger.info(
                            "Fetched all data for year, now filtering by parameter",
                            year=year,
                            total_records=year_total
                        )

                        param_codes = [param['code'] for param in parameters_to_save]
                        param_counts = {}

                        # Process each parameter separately
                        for param in parameters_to_save:
                            param_code = param['code']
                            param_name = param['name']

                            # Filter data for this specific parameter
                            param_filtered = [
                                row for row in year_results
                                if row.get('code_param') == param_code
                            ]

                            param_count = len(param_filtered)
                            param_counts[param_name] = param_count
                            total_records += param_count

                            if param_filtered:
                                # Build output filename: qualite_nappes_monthly_no3.parquet (no region suffix)
                                merged_output_path = output_base_path.parent / f"{output_base_path.stem}_{param_name}.parquet"

                                # Check if merged file exists - if so, append to it
                                if merged_output_path.exists():
                                    self.logger.info(
                                        f"Appending {param_name.upper()} data to existing merged file",
                                        year=year,
                                        records=param_count,
                                        file=merged_output_path.name
                                    )
                                    # Load existing data
                                    existing_df = pd.read_parquet(merged_output_path)
                                    # Append new data
                                    new_df = pd.DataFrame(param_filtered)
                                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                                    # Sort by date for consistency
                                    if 'date_debut_prelevement' in combined_df.columns:
                                        combined_df = combined_df.sort_values('date_debut_prelevement')
                                    # Save back
                                    combined_df.to_parquet(merged_output_path, index=False, engine='pyarrow')

                                    self.logger.info(
                                        f"Appended {param_name.upper()} data",
                                        year=year,
                                        new_records=param_count,
                                        total_records=len(combined_df),
                                        output=str(merged_output_path)
                                    )
                                else:
                                    # Create new merged file
                                    export_to_parquet(param_filtered, merged_output_path)

                                    self.logger.info(
                                        f"Created new {param_name.upper()} merged file",
                                        year=year,
                                        records=param_count,
                                        output=str(merged_output_path)
                                    )

                        if param_counts:
                            filter_pct = (sum(param_counts.values()) / year_total * 100) if year_total > 0 else 0

                            self.logger.info(
                                "Yearly data filtered and saved (separate parameters)",
                                year=year,
                                total_fetched=year_total,
                                filtered_records=sum(param_counts.values()),
                                kept_percentage=f"{filter_pct:.2f}%",
                                parameters_saved=param_counts
                            )
                        else:
                            self.logger.warning(
                                "No data for requested parameters",
                                year=year,
                                total_fetched=year_total,
                                requested_params=param_codes
                            )

                        # Clear memory
                        year_results = []

            self.state_manager.complete_task(task_id, {'records': total_records})
            self.logger.info(
                "Task completed",
                task_id=task_id,
                records=total_records,
                iteration_mode=iteration_mode
            )

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
        # Log run start with metadata
        self.logger.info(
            "Run started",
            playbook=str(self.playbook_path),
            skip_completed=self.skip_completed,
            tasks_to_run=self.tasks_to_run,
            total_tasks=len(self.playbook.get('tasks', []))
        )

        self.logger.info("Starting orchestrator")

        # Track execution statistics
        tasks_completed = 0
        tasks_failed = 0
        tasks_skipped = 0

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
                try:
                    if not self.should_run_task(task_id):
                        tasks_skipped += 1
                        continue
                    task_methods[task_id](task)
                    tasks_completed += 1
                except Exception as e:
                    tasks_failed += 1
                    self.logger.error("Task failed", task_id=task_id, error=str(e))

        # Post-processing
        self.run_post_processing()

        # Calculate run duration
        run_duration = time.time() - self.run_start_time

        # Log run completion with statistics
        self.logger.info(
            "Run completed",
            duration_seconds=round(run_duration, 2),
            tasks_completed=tasks_completed,
            tasks_failed=tasks_failed,
            tasks_skipped=tasks_skipped
        )

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
