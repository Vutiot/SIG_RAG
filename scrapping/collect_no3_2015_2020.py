#!/usr/bin/env python3
"""Targeted script to collect NO3 data for 2015-2020."""

import time
from datetime import datetime, timedelta
from pathlib import Path

from scrapers.api_scrapers import HubeauQualiteNappesAPI
from utils.state_manager import StateManager
from utils.rate_limiter import RateLimiter
from utils.export import export_to_parquet
import structlog

logger = structlog.get_logger(__name__)

def main():
    # Initialize
    state_manager = StateManager("metadata/state.db")
    rate_limiter = RateLimiter({
        "hubeau.eaufrance.fr": 5
    })

    task_id = "t6"
    api = HubeauQualiteNappesAPI(
        task_id=task_id,
        state_manager=state_manager,
        rate_limiter=rate_limiter
    )

    output_base_path = Path("exports/parquet/qualite_nappes.parquet")

    # Process NO3 for 2015-2020 only
    for year in range(2015, 2021):  # 2015 to 2020
        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31)

        logger.info(
            "Processing year",
            parameter="NO3",
            year=year,
            start=year_start.strftime('%Y-%m-%d'),
            end=year_end.strftime('%Y-%m-%d')
        )

        year_results = []

        # Generate daily date ranges for this year
        current_date = year_start
        while current_date <= year_end:
            day_str = current_date.strftime('%Y-%m-%d')

            logger.info(
                "Fetching groundwater analyses",
                parameter="NO3",
                date=day_str
            )

            results = api.get_analyses(
                code_parametre="NO3",
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
            yearly_output_path = output_base_path.parent / f"{output_base_path.stem}_NO3_{year}.parquet"
            export_to_parquet(year_results, yearly_output_path)

            logger.info(
                "Yearly data saved",
                parameter="NO3",
                year=year,
                records=len(year_results),
                output=str(yearly_output_path)
            )

            # Clear memory
            year_results = []
        else:
            logger.warning(
                "No data found for year",
                parameter="NO3",
                year=year
            )

    logger.info("Collection complete for NO3 2015-2020")

if __name__ == "__main__":
    main()
