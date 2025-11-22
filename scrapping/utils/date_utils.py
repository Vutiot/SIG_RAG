"""
Date utility functions for API scraping with different granularities.

Provides functions to generate date ranges for iterative API querying
with daily, monthly, or yearly granularity to balance API performance
and pagination limits.
"""

from datetime import datetime, timedelta
from typing import List, Tuple
from dateutil.relativedelta import relativedelta


def parse_period(period_str: str) -> Tuple[datetime, datetime]:
    """
    Parse a period string in format 'YYYY-MM-DD/YYYY-MM-DD' into datetime objects.

    Args:
        period_str: Period string like "2015-01-01/2025-12-31"

    Returns:
        Tuple of (start_date, end_date) as datetime objects

    Example:
        >>> start, end = parse_period("2015-01-01/2025-12-31")
        >>> print(start)
        2015-01-01 00:00:00
    """
    date_min_str, date_max_str = period_str.split('/')
    start_date = datetime.strptime(date_min_str, '%Y-%m-%d')
    end_date = datetime.strptime(date_max_str, '%Y-%m-%d')
    return start_date, end_date


def generate_date_ranges(
    start_date: datetime,
    end_date: datetime,
    granularity: str = 'monthly'
) -> List[Tuple[datetime, datetime]]:
    """
    Generate a list of date ranges with specified granularity.

    Args:
        start_date: Start of the overall period
        end_date: End of the overall period
        granularity: One of 'daily', 'monthly', 'yearly'

    Returns:
        List of (range_start, range_end) tuples covering the full period

    Examples:
        >>> start = datetime(2015, 1, 1)
        >>> end = datetime(2015, 3, 15)
        >>> ranges = generate_date_ranges(start, end, 'monthly')
        >>> len(ranges)
        3
        >>> # Returns: [(2015-01-01, 2015-01-31), (2015-02-01, 2015-02-28), (2015-03-01, 2015-03-15)]

        >>> ranges = generate_date_ranges(start, end, 'yearly')
        >>> len(ranges)
        1
        >>> # Returns: [(2015-01-01, 2015-03-15)]
    """
    if granularity not in ('daily', 'monthly', 'yearly'):
        raise ValueError(f"Invalid granularity '{granularity}'. Must be 'daily', 'monthly', or 'yearly'.")

    ranges = []
    current = start_date

    if granularity == 'daily':
        while current <= end_date:
            ranges.append((current, current))
            current += timedelta(days=1)

    elif granularity == 'monthly':
        while current <= end_date:
            # Calculate the last day of the current month
            next_month = current + relativedelta(months=1)
            month_end = next_month - timedelta(days=1)

            # Don't exceed the overall end_date
            range_end = min(month_end, end_date)
            ranges.append((current, range_end))

            current = next_month

    elif granularity == 'yearly':
        while current <= end_date:
            # Calculate the last day of the current year
            year_end = datetime(current.year, 12, 31)

            # Don't exceed the overall end_date
            range_end = min(year_end, end_date)
            ranges.append((current, range_end))

            # Move to next year
            current = datetime(current.year + 1, 1, 1)

    return ranges


def get_iteration_mode_from_params(params: dict) -> str:
    """
    Determine iteration mode from task parameters with backward compatibility.

    Args:
        params: Task parameters dictionary from playbook

    Returns:
        One of 'daily', 'monthly', 'yearly'

    Behavior:
        - If 'iteration_mode' is specified, use it directly
        - Else if 'use_daily_iteration' is True, use 'daily'
        - Else if 'nom_region' is specified (without daily iteration), use 'yearly'
        - Default: 'monthly'
    """
    # New unified parameter takes precedence
    if 'iteration_mode' in params:
        mode = params['iteration_mode']
        if mode not in ('daily', 'monthly', 'yearly'):
            raise ValueError(f"Invalid iteration_mode '{mode}'. Must be 'daily', 'monthly', or 'yearly'.")
        return mode

    # Backward compatibility with old flags
    if params.get('use_daily_iteration', False):
        return 'daily'

    # Old behavior: region filter implied yearly iteration
    if params.get('nom_region') and not params.get('use_daily_iteration'):
        return 'yearly'

    # Default to monthly as a balanced approach
    return 'monthly'


def format_date_for_api(date: datetime) -> str:
    """
    Format a datetime object for API query parameters.

    Args:
        date: Datetime object to format

    Returns:
        Date string in 'YYYY-MM-DD' format

    Example:
        >>> dt = datetime(2015, 3, 5)
        >>> format_date_for_api(dt)
        '2015-03-05'
    """
    return date.strftime('%Y-%m-%d')
