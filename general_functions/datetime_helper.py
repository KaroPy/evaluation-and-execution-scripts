from datetime import timezone
import pandas as pd


def transform_date_to_timestamp_milliseconds(date):
    """
    Function to transform datesting in
    timestamp with milliseconds
    Args:
        date: date string
    """
    date = pd.to_datetime(date).replace(tzinfo=timezone.utc).timestamp() * 1000
    date = int(date)
    return date
