import pandas as pd


def add_columns(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["start_time"]).dt.date
    df["week"] = pd.to_datetime(df["start_time"]).dt.isocalendar().week
    df["year"] = pd.to_datetime(df["start_time"]).dt.year
    df["year-week"] = df["year"].astype(str) + "-W" + df["week"].astype(str)
    df["year-month"] = (
        df["year"].astype(str)
        + "-"
        + pd.to_datetime(df["start_time"]).dt.month.astype(str).str.zfill(2)
    )
    return df
