import pytz
from datetime import datetime, timezone
import pandas as pd


def transform_local_time_to_datetime(
    date: str, dateformat: str, use_timezone="Europe/Berlin"
):
    local = pytz.timezone(use_timezone)
    naive = datetime.strptime(date, dateformat)
    local_dt = local.localize(naive, is_dst=None)
    utc_dt = local_dt.astimezone(pytz.utc)
    utc_dt = utc_dt.strftime(dateformat)
    print(local_dt, utc_dt)
    return utc_dt


def timestamp_milliseconds(date):
    """
    Function to transform datesting in
    timestamp with milliseconds
    Args:
        date: date string
    """
    date = pd.to_datetime(date).replace(tzinfo=timezone.utc).timestamp() * 1000
    date = int(date)
    return date
