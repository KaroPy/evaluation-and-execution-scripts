"""Merge DTW daily incident rows with Pylon DTW tickets."""

from __future__ import annotations

import pandas as pd

PYLON_MERGE_COLS = [
    "issue_id",
    "number",
    "title",
    "state",
    "created_at",
    "created_date",
    "updated_at",
    "tags",
    "assignee",
    "signal_id",
    "treatment_id",
    "workspace_id",
    "incident_date",
    "dtw_label",
    "other_custom_fields",
    "issue_url",
]

# Priority: exact date, then pylon created_at 1 day after dtw date, then 1 day before.
DATE_MATCH_PRIORITY: list[tuple[int, str]] = [
    (0, "exact"),
    (1, "+1d"),
    (-1, "-1d"),
]


def _prepare_frames(
    dtw_daily: pd.DataFrame,
    pylon: pd.DataFrame,
    workspace_ids: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dtw = dtw_daily.copy()
    pylon_df = pylon.copy()

    if workspace_ids is not None and "workspace_id" not in dtw.columns:
        dtw["workspace_id"] = dtw["account_name"].map(workspace_ids)
    elif workspace_ids is not None and dtw["workspace_id"].isna().any():
        dtw["workspace_id"] = dtw["workspace_id"].fillna(
            dtw["account_name"].map(workspace_ids)
        )

    dtw["date"] = pd.to_datetime(dtw["date"]).dt.normalize()

    pylon_df["created_at"] = pd.to_datetime(pylon_df["created_at"], utc=True)
    pylon_df["created_date"] = (
        pylon_df["created_at"].dt.tz_convert(None).dt.normalize()
    )
    return dtw, pylon_df


def filter_pylon_to_dtw_daterange(
    pylon: pd.DataFrame,
    dtw_daily: pd.DataFrame,
    pad_days: int = 1,
) -> pd.DataFrame:
    """
    Keep pylon tickets whose ``created_at`` date falls within the
    ``dtw_daily.date`` range (optionally padded by ``pad_days`` to allow
    ±1 day matching at the boundaries).
    """
    dtw_dates = pd.to_datetime(dtw_daily["date"]).dt.normalize()
    dtw_date_min, dtw_date_max = dtw_dates.min(), dtw_dates.max()

    pylon_df = pylon.copy()
    if "created_date" not in pylon_df.columns:
        pylon_df["created_at"] = pd.to_datetime(pylon_df["created_at"], utc=True)
        pylon_df["created_date"] = (
            pylon_df["created_at"].dt.tz_convert(None).dt.normalize()
        )

    return pylon_df[
        pylon_df["created_date"].between(
            dtw_date_min - pd.Timedelta(days=pad_days),
            dtw_date_max + pd.Timedelta(days=pad_days),
        )
    ].copy()


def merge_dtw_daily_with_pylon(
    dtw_daily: pd.DataFrame,
    pylon: pd.DataFrame,
    workspace_ids: dict[str, str] | None = None,
    pad_days: int = 1,
    match_on: str = "signal",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Left-merge dtw_daily with pylon tickets.

    Pylon tickets are first filtered to the ``dtw_daily`` date range
    (padded by ``pad_days``).

    Match keys depend on ``match_on``:
    - ``signal`` (default): dtw ``audience_id`` ↔ pylon ``signal_id``
    - ``treatment``: dtw ``treatment_id`` ↔ pylon ``treatment_id``

    Always also matches ``workspace_id`` and date(pylon.created_at) vs
    dtw_daily.date with priority: exact → +1d → -1d.

    Each pylon ticket is assigned at most once (highest-priority date match).

    If ``workspace_ids`` is provided and ``workspace_id`` is missing on
    ``dtw_daily``, it is mapped from ``account_name``.

    Returns
    -------
    dtw_daily_pylon :
        Left join of dtw_daily with matching pylon columns. Includes
        ``pylon_date_match`` (``exact`` / ``+1d`` / ``-1d``).
    pylon_unmatched :
        In-range pylon tickets that did not match any dtw_daily row.
    """
    if match_on == "signal":
        dtw_id_col, pylon_id_col = "audience_id", "signal_id"
    elif match_on == "treatment":
        dtw_id_col, pylon_id_col = "treatment_id", "treatment_id"
    else:
        raise ValueError("match_on must be 'signal' or 'treatment'")

    dtw, pylon_df = _prepare_frames(dtw_daily, pylon, workspace_ids)
    if dtw_id_col not in dtw.columns:
        raise ValueError(f"dtw_daily missing column required for merge: {dtw_id_col}")
    if pylon_id_col not in pylon_df.columns:
        raise ValueError(f"pylon missing column required for merge: {pylon_id_col}")

    pylon_in_range = filter_pylon_to_dtw_daterange(pylon_df, dtw, pad_days=pad_days)

    merge_cols = [c for c in PYLON_MERGE_COLS if c in pylon_in_range.columns]
    dtw_keys = (
        dtw[[dtw_id_col, "workspace_id", "date"]]
        .drop_duplicates()
        .rename(columns={dtw_id_col: "_match_id", "date": "_dtw_match_date"})
    )

    assigned_parts: list[pd.DataFrame] = []
    remaining_ids = set(pylon_in_range["issue_id"].dropna())

    for offset, label in DATE_MATCH_PRIORITY:
        if not remaining_ids:
            break

        cand = pylon_in_range[pylon_in_range["issue_id"].isin(remaining_ids)].copy()
        cand["_dtw_match_date"] = cand["created_date"] - pd.Timedelta(days=offset)
        cand["pylon_date_match"] = label
        cand["_match_id"] = cand[pylon_id_col]

        hit = cand.merge(
            dtw_keys,
            on=["_match_id", "workspace_id", "_dtw_match_date"],
            how="inner",
        )
        if hit.empty:
            continue

        hit = hit.drop_duplicates(subset=["issue_id"], keep="first")
        assigned_parts.append(
            hit[merge_cols + ["_match_id", "_dtw_match_date", "pylon_date_match"]]
        )
        remaining_ids -= set(hit["issue_id"])

    if assigned_parts:
        pylon_assigned = pd.concat(assigned_parts, ignore_index=True)
    else:
        pylon_assigned = pd.DataFrame(
            columns=merge_cols + ["_match_id", "_dtw_match_date", "pylon_date_match"]
        )

    dtw_daily_pylon = dtw.merge(
        pylon_assigned,
        left_on=[dtw_id_col, "workspace_id", "date"],
        right_on=["_match_id", "workspace_id", "_dtw_match_date"],
        how="left",
        indicator="pylon_merge",
        suffixes=("", "_pylon"),
    )

    matched_issue_ids = set(
        dtw_daily_pylon.loc[dtw_daily_pylon["issue_id"].notna(), "issue_id"]
    )
    pylon_unmatched = pylon_in_range[
        ~pylon_in_range["issue_id"].isin(matched_issue_ids)
    ].copy()

    return dtw_daily_pylon, pylon_unmatched


def annotate_pylon_unmatched_reasons(
    pylon_unmatched: pd.DataFrame,
    dtw_daily: pd.DataFrame,
    match_on: str = "signal",
) -> pd.DataFrame:
    """
    Add an ``unmatched_reason`` column explaining why each pylon ticket
    did not merge into ``dtw_daily``.

    Expects ``pylon_unmatched`` to already be filtered to the dtw date range.
    """
    if match_on == "signal":
        dtw_id_col, pylon_id_col = "audience_id", "signal_id"
        missing_msg = "missing signal_id/workspace_id"
        no_key_msg = "no matching audience_id+workspace_id in dtw_daily"
    elif match_on == "treatment":
        dtw_id_col, pylon_id_col = "treatment_id", "treatment_id"
        missing_msg = "missing treatment_id/workspace_id"
        no_key_msg = "no matching treatment_id+workspace_id in dtw_daily"
    else:
        raise ValueError("match_on must be 'signal' or 'treatment'")

    dtw = dtw_daily.copy()
    dtw["date"] = pd.to_datetime(dtw["date"]).dt.normalize()

    unmatched = pylon_unmatched.copy()
    if "created_date" not in unmatched.columns:
        unmatched["created_at"] = pd.to_datetime(unmatched["created_at"], utc=True)
        unmatched["created_date"] = (
            unmatched["created_at"].dt.tz_convert(None).dt.normalize()
        )

    unmatched_reason: list[str] = []
    for _, row in unmatched.iterrows():
        reasons: list[str] = []
        if pd.isna(row.get(pylon_id_col)) or pd.isna(row["workspace_id"]):
            reasons.append(missing_msg)
        elif dtw_id_col not in dtw.columns:
            reasons.append(f"dtw_daily missing {dtw_id_col}")
        else:
            key_match = dtw[
                (dtw[dtw_id_col] == row[pylon_id_col])
                & (dtw["workspace_id"] == row["workspace_id"])
            ]
            if key_match.empty:
                reasons.append(no_key_msg)
            else:
                deltas = (row["created_date"] - key_match["date"]).dt.days.abs()
                if not (deltas <= 1).any():
                    reasons.append(
                        f"{dtw_id_col}/workspace found, but created_at not within ±1 day of dtw date"
                    )
        unmatched_reason.append("; ".join(reasons) if reasons else "unknown")

    return unmatched.assign(unmatched_reason=unmatched_reason)
