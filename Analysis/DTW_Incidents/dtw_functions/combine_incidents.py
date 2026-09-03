"""Combine daily DTW incident flags into incident / non-incident periods."""

from __future__ import annotations

import pandas as pd

DEFAULT_END_FALSE_DAYS = 3


def _episode_bounds_for_group(
    group: pd.DataFrame,
    end_false_days: int = DEFAULT_END_FALSE_DAYS,
) -> list[dict]:
    """
    Detect incident episodes in a sorted daily series.

    - start_date: first ``is_incident`` True after a False (or series start)
    - end_date: last True before ``end_false_days`` consecutive False rows
      (or last True if the series ends while still in an incident)
    """
    g = group.sort_values("date")
    episodes: list[dict] = []

    in_incident = False
    start_date = None
    last_true_date = None
    consecutive_false = 0

    for _, row in g.iterrows():
        if bool(row["is_incident"]):
            if not in_incident:
                start_date = row["date"]
                in_incident = True
            last_true_date = row["date"]
            consecutive_false = 0
            continue

        if not in_incident:
            consecutive_false = 0
            continue

        consecutive_false += 1
        if consecutive_false >= end_false_days:
            episodes.append(
                {
                    "start_date": start_date,
                    "end_date": last_true_date,
                    "period_type": "incident",
                }
            )
            in_incident = False
            start_date = None
            last_true_date = None
            consecutive_false = 0

    if in_incident and last_true_date is not None:
        episodes.append(
            {
                "start_date": start_date,
                "end_date": last_true_date,
                "period_type": "incident",
            }
        )

    return episodes


def _no_incident_bounds(
    daily: pd.DataFrame,
    incident_bounds: list[dict],
) -> list[dict]:
    """
    Build complementary no-incident ranges over the observed date span.

    Covers days before the first incident, gaps between incidents, and days
    after the last incident (including the consecutive False days that close
    an episode).
    """
    if daily.empty:
        return []

    span_start = daily["date"].min()
    span_end = daily["date"].max()
    no_incident: list[dict] = []

    cursor = span_start
    for episode in sorted(incident_bounds, key=lambda e: e["start_date"]):
        gap_end = episode["start_date"] - pd.Timedelta(days=1)
        if cursor <= gap_end:
            no_incident.append(
                {
                    "start_date": cursor,
                    "end_date": gap_end,
                    "period_type": "no_incident",
                }
            )
        cursor = episode["end_date"] + pd.Timedelta(days=1)

    if cursor <= span_end:
        no_incident.append(
            {
                "start_date": cursor,
                "end_date": span_end,
                "period_type": "no_incident",
            }
        )

    return no_incident


def _aggregate_pylon_fields(rows: pd.DataFrame) -> dict:
    """Collect non-null dtw_label / tags within an episode window."""
    labels: list[str] = []
    if "dtw_label" in rows.columns:
        labels = (
            rows["dtw_label"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )

    tags: list[str] = []
    if "tags" in rows.columns:
        exploded = (
            rows["tags"]
            .dropna()
            .astype(str)
            .str.split("|")
            .explode()
            .str.strip()
        )
        tags = (
            exploded.loc[exploded != ""]
            .drop_duplicates()
            .tolist()
        )

    return {
        "dtw_label": "|".join(labels) if labels else pd.NA,
        "tags": "|".join(tags) if tags else pd.NA,
        "n_pylon_tickets": (
            int(rows["issue_id"].nunique())
            if "issue_id" in rows.columns
            else 0
        ),
    }


def combine_incidents(
    dtw_daily_pylon: pd.DataFrame,
    end_false_days: int = DEFAULT_END_FALSE_DAYS,
    entity_id_col: str = "audience_id",
) -> pd.DataFrame:
    """
    Combine daily incident flags into periods per account/entity.

    Groups by ``account_name`` and ``entity_id_col`` (default ``audience_id``;
    use ``treatment_id`` for treatment-based daily data). An incident period
    starts on the first incident day after a non-incident day, and ends after
    ``end_false_days`` consecutive non-incident days (default 3).

    Also returns complementary ``no_incident`` periods covering the rest of
    each account/entity date span.

    Pylon fields ``dtw_label`` and ``tags`` from ``dtw_daily_pylon`` that fall
    within ``[start_date, end_date]`` are aggregated onto each period.

    Parameters
    ----------
    dtw_daily_pylon :
        Daily DTW rows, optionally already merged with pylon ticket fields.
        Required columns: ``account_name``, ``entity_id_col``, ``date``,
        ``is_incident``.
    end_false_days :
        Consecutive False days required to close an incident episode.
    entity_id_col :
        Entity key to group by (``audience_id`` or ``treatment_id``).

    Returns
    -------
    pd.DataFrame
        One row per period (``period_type`` = ``incident`` or ``no_incident``)
        with start/end dates and aggregated pylon ``dtw_label`` / ``tags``.
    """
    required = {"account_name", entity_id_col, "date", "is_incident"}
    missing = required - set(dtw_daily_pylon.columns)
    if missing:
        raise ValueError(f"dtw_daily_pylon missing columns: {sorted(missing)}")

    df = dtw_daily_pylon.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["is_incident"] = df["is_incident"].astype(bool)

    sort_cols = ["account_name", entity_id_col, "date"]
    df = df.sort_values(sort_cols)

    episode_rows: list[dict] = []
    group_cols = ["account_name", entity_id_col]

    for keys, group in df.groupby(group_cols, sort=False):
        account_name, entity_id = keys
        meta = {
            "account_name": account_name,
            entity_id_col: entity_id,
        }
        if "model_type" in group.columns and group["model_type"].notna().any():
            meta["model_type"] = group["model_type"].dropna().iloc[0]
        if "workspace_id" in group.columns and group["workspace_id"].notna().any():
            meta["workspace_id"] = group["workspace_id"].dropna().iloc[0]

        daily = (
            group.sort_values(["date", "is_incident"], ascending=[True, False])
            .drop_duplicates(subset=["date"], keep="first")
        )

        incident_bounds = _episode_bounds_for_group(
            daily, end_false_days=end_false_days
        )
        all_bounds = incident_bounds + _no_incident_bounds(daily, incident_bounds)

        for bounds in all_bounds:
            window = group[
                (group["date"] >= bounds["start_date"])
                & (group["date"] <= bounds["end_date"])
            ]
            incident_days = int(
                daily.loc[
                    (daily["date"] >= bounds["start_date"])
                    & (daily["date"] <= bounds["end_date"]),
                    "is_incident",
                ].sum()
            )
            pylon_fields = _aggregate_pylon_fields(window)
            episode_rows.append(
                {
                    **meta,
                    **bounds,
                    "incident_days": incident_days,
                    "duration_days": int(
                        (bounds["end_date"] - bounds["start_date"]).days + 1
                    ),
                    **pylon_fields,
                }
            )

    empty_cols = [
        "account_name",
        entity_id_col,
        "model_type",
        "workspace_id",
        "start_date",
        "end_date",
        "period_type",
        "incident_days",
        "duration_days",
        "dtw_label",
        "tags",
        "n_pylon_tickets",
    ]
    if not episode_rows:
        return pd.DataFrame(columns=empty_cols)

    result = pd.DataFrame(episode_rows)
    result = result.sort_values(
        ["account_name", entity_id_col, "start_date"]
    ).reset_index(drop=True)
    return result
