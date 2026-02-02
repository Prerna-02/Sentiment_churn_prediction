# services/dashboard_api/app/utils/time_utils.py

from datetime import datetime, timedelta, timezone
from typing import Tuple, Optional


def parse_time_window(time_window: str) -> Tuple[datetime, str]:
    """
    Parse time window string and return (start_time, granularity).
    
    Args:
        time_window: "1h", "24h", "7d", "30d", "all"
    
    Returns:
        (start_datetime, granularity)
    """
    now = datetime.now(timezone.utc)
    
    if time_window == "1h":
        start_time = now - timedelta(hours=1)
        granularity = "5m"
    elif time_window == "24h":
        start_time = now - timedelta(hours=24)
        granularity = "1h"
    elif time_window == "7d":
        start_time = now - timedelta(days=7)
        granularity = "1d"
    elif time_window == "30d":
        start_time = now - timedelta(days=30)
        granularity = "1d"
    elif time_window == "all":
        # Set to a very old date (essentially no time filter)
        start_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
        granularity = "1d"
    else:
        # Default to 24h
        start_time = now - timedelta(hours=24)
        granularity = "1h"
    
    return start_time, granularity


def get_granularity_milliseconds(granularity: str) -> int:
    """
    Convert granularity string to milliseconds for MongoDB date binning.
    
    Args:
        granularity: "5m", "1h", "1d"
    
    Returns:
        Milliseconds
    """
    if granularity == "5m":
        return 5 * 60 * 1000  # 5 minutes
    elif granularity == "1h":
        return 60 * 60 * 1000  # 1 hour
    elif granularity == "1d":
        return 24 * 60 * 60 * 1000  # 1 day
    else:
        return 60 * 60 * 1000  # Default to 1 hour


def get_time_filter(time_window: str, product_id: Optional[str] = None, channel: Optional[str] = None) -> dict:
    """
    Build MongoDB filter object for time window and optional filters.
    
    Args:
        time_window: Time window string
        product_id: Optional product filter
        channel: Optional channel filter
    
    Returns:
        MongoDB filter dict
    """
    start_time, _ = parse_time_window(time_window)
    
    # Base filter: time range
    filter_query = {
        "timestamp_utc": {"$gte": start_time.isoformat()}
    }
    
    # Add optional filters
    if product_id:
        filter_query["product_id"] = product_id
    
    if channel:
        filter_query["channel"] = channel
    
    return filter_query


def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO string with timezone."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


