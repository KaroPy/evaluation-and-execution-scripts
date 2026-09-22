"""
Check which models from the last N months were trained with reset_lstm=True.

For each workspace:
  1. Query models created in the lookback window (default: 6 months)
  2. Deduplicate by model.path (keep the newest model.created)
  3. Read reset_lstm.json from the targeting S3 prefix, rewriting
     best_models -> new_models in model.path (the flag lives under new_models)
  4. Mark reset=True only when that file is {"reset_lstm": true}

Writes:
  - a detailed model table (model.id, model.path, model.type, reset, signal fields)
  - a signal-level results table: signal.name | signal.source | model.audience,
    reset count / percentage, and days between resets (list, avg, sd)

Usage (from repo root):
    python scripts/models/check_model_resets.py
    python scripts/models/check_model_resets.py --workspace More --months 6
"""

from __future__ import annotations

import argparse
import calendar
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.call_api_with_account_id import (  # ruff: ignore[module-import-not-at-top-of-file]
    make_http_post_call,
    validate_response,
)
from general_functions.conncet_s3 import S3Connection  # ruff: ignore[module-import-not-at-top-of-file]
from general_functions.constants import return_api_url  # ruff: ignore[module-import-not-at-top-of-file]
from general_functions.return_workspace_ids import return_workspace_ids  # ruff: ignore[module-import-not-at-top-of-file]
from general_functions.sanitize_accout_name import sanitize_account_name  # ruff: ignore[module-import-not-at-top-of-file]

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
RESET_LSTM_FILENAME = "reset_lstm.json"
BEST_MODELS_DIR = "best_models"
NEW_MODELS_DIR = "new_models"
MISSING_S3_CODES = {"NoSuchKey", "404", "NotFound", "NoSuchBucket"}
PATH_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")
CAUSAL_TYPE = "causal"
CONVERSION_TYPE = "conversion"
CROSS_WORKSPACE_TYPES = (CONVERSION_TYPE, CAUSAL_TYPE)

MODEL_COLUMNS = [
    "workspace.name",
    "workspace.id",
    "model.id",
    "model.path",
    "model.type",
    "model.audience",
    "model.objective",
    "signal.name",
    "signal.source",
    "reset.date",
    "reset",
]
RESULT_COLUMNS = [
    "workspace.name",
    "model.type",
    "signal.name",
    "signal.source",
    "model.audience",
    "model.objective",
    "models",
    "resets",
    "reset_pct",
    "days_between",
    "days_between_avg",
    "days_between_sd",
]
ALL_WORKSPACES = "(all workspaces)"
SIGNAL_TOTAL = "(total)"
SIGNAL_WORKSPACE_AVG = "(workspace avg)"


def default_output_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"model_resets_{stamp}.csv"


def default_summary_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}_summary{output_path.suffix}")


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"check_model_resets_{stamp}.log"


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root_logger.addHandler(file_handler)
    logging.getLogger("check_model_resets.api").setLevel(logging.WARNING)


def normalize_workspace_filter(workspaces: list[str] | None) -> set[str] | None:
    if not workspaces:
        return None
    names: set[str] = set()
    for item in workspaces:
        for part in item.split(","):
            name = part.strip()
            if name:
                names.add(name)
    return names or None


def months_ago(months: int, now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    month = current.month - months
    year = current.year
    while month <= 0:
        month += 12
        year -= 1
    day = min(current.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, tzinfo=timezone.utc).strftime("%Y-%m-%d")


def api_query_url(api_url: str, resource: str) -> str:
    base = api_url.rstrip("/")
    if base.endswith("/api"):
        return f"{base}/{resource}/query"
    return f"{base}/api/{resource}/query"


def targeting_bucket(workspace_name: str, model_path: str | None = None) -> str:
    if model_path:
        root = model_path.split("/")[0]
        for marker in ("-aud-", "-conversion-"):
            if marker in root:
                return f"innkeepr-targeting-{root.split(marker, 1)[0]}"
    return f"innkeepr-targeting-{sanitize_account_name(workspace_name)}"


def reset_lstm_prefix(model_path: str) -> str:
    """reset_lstm.json lives under new_models, not best_models."""
    return model_path.rstrip("/").replace(BEST_MODELS_DIR, NEW_MODELS_DIR)


def reset_lstm_key(model_path: str) -> str:
    return f"{reset_lstm_prefix(model_path)}/{RESET_LSTM_FILENAME}"


def is_reset_lstm_true(payload: object) -> bool:
    """True only for {"reset_lstm": true}; missing file, false, or other values are False."""
    return isinstance(payload, dict) and payload.get("reset_lstm") is True


def s3_error_code(exc: ClientError) -> str:
    return str((exc.response or {}).get("Error", {}).get("Code", ""))


def query_all(
    endpoint_url: str,
    account_id: str,
    content: dict,
    logger: logging.Logger,
) -> list[dict]:
    logger.debug("Querying %s content=%s", endpoint_url, content)
    next_page = 1
    data: list[dict] = []
    while next_page is not None:
        payload = json.dumps(
            {
                "content": content,
                "pagination": {"page": next_page},
                "context": {"accountId": account_id},
            }
        )
        json_body = make_http_post_call(endpoint_url, payload, logger)
        validate_response(json_body, logger)
        data.extend(json_body["data"])
        next_page = (json_body.get("pagination") or {}).get("next")
    return data


def query_all_or_empty(
    endpoint_url: str,
    account_id: str,
    content: dict,
    logger: logging.Logger,
    label: str,
) -> list[dict]:
    try:
        return query_all(endpoint_url, account_id, content, logger)
    except Exception as exc:
        logger.warning("Failed querying %s for %s: %s", label, account_id, exc)
        return []


def connection_names_by_id(connections: list[dict]) -> dict[str, str]:
    names: dict[str, str] = {}
    for connection in connections:
        connection_id = connection.get("id") or connection.get("_id")
        name = connection.get("name")
        if connection_id and name:
            names[str(connection_id)] = str(name)
    return names


def platform_name_from_source(source: object, source_names: dict[str, str]) -> str:
    """Prefer connection.platform.name, e.g. googleAnalytics, not the raw source object."""
    if source is None:
        return ""
    if isinstance(source, dict):
        platform = source.get("platform")
        if isinstance(platform, dict) and platform.get("name"):
            return str(platform["name"])
        if source.get("name"):
            return str(source["name"])
        source_id = source.get("id")
        if source_id:
            return source_names.get(str(source_id), str(source_id))
        return ""
    text = str(source).strip()
    if not text:
        return ""
    return source_names.get(text, text)


def build_signal_lookup(
    signals: list[dict],
    connections: list[dict],
) -> dict[str, dict[str, str]]:
    source_names = connection_names_by_id(connections)
    lookup: dict[str, dict[str, str]] = {}
    for signal in signals:
        signal_id = signal.get("id")
        if not signal_id:
            continue
        source = signal.get("source") or signal.get("connection")
        lookup[str(signal_id)] = {
            "signal.name": signal.get("name") or "",
            "signal.source": platform_name_from_source(source, source_names),
        }
    return lookup


def signal_fields_for_audience(
    audience_id: str,
    lookup: dict[str, dict[str, str]],
) -> tuple[str, str]:
    if audience_id in {"", "(none)"}:
        return "", ""
    info = lookup.get(audience_id) or {}
    return info.get("signal.name") or "", info.get("signal.source") or ""


def first_signal_field(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df.columns:
        return ""
    for value in df[column]:
        text = normalize_label(value)
        if text != "(none)":
            return text
    return ""


def deduplicate_by_path(models: list[dict]) -> tuple[list[dict], int]:
    """Keep the newest model.created per non-empty model.path."""
    by_path: dict[str, dict] = {}
    skipped_empty_path = 0

    for model in models:
        path = (model.get("path") or "").strip()
        if not path:
            skipped_empty_path += 1
            continue
        normalized = path.rstrip("/")
        existing = by_path.get(normalized)
        if existing is None or (model.get("created") or "") > (existing.get("created") or ""):
            by_path[normalized] = model

    return list(by_path.values()), skipped_empty_path


def read_reset_flag(
    s3: S3Connection,
    bucket: str,
    model_path: str,
    cache: dict[tuple[str, str], bool],
    logger: logging.Logger,
) -> bool:

    key = reset_lstm_key(model_path)
    cache_key = (bucket, key)
    if cache_key in cache:
        return cache[cache_key]

    try:
        raw = s3.read_json_from_aws(bucket, key)
        payload = json.loads(raw)
        reset = is_reset_lstm_true(payload)
    except ClientError as exc:
        code = s3_error_code(exc)
        if code in MISSING_S3_CODES:
            reset = False
        else:
            logger.warning(
                "S3 error reading s3://%s/%s (%s) — treating as False", bucket, key, code
            )
            reset = False
    except json.JSONDecodeError:
        logger.warning("Invalid JSON at s3://%s/%s — treating as False", bucket, key)
        reset = False
    except Exception as exc:
        logger.warning("Failed reading s3://%s/%s (%s) — treating as False", bucket, key, exc)
        reset = False

    cache[cache_key] = reset
    return reset


def date_from_model_path(path: str | None) -> str | None:
    if not path:
        return None
    normalized = path.rstrip("/")
    folder = normalized.rsplit("/", 1)[-1] if "/" in normalized else normalized
    match = PATH_DATE_PATTERN.search(folder)
    if match:
        return match.group(1)
    match = PATH_DATE_PATTERN.search(normalized)
    return match.group(1) if match else None


def date_from_created(created: object) -> str | None:
    if created is None or (isinstance(created, float) and pd.isna(created)):
        return None
    text = str(created).strip()
    if len(text) >= 10 and PATH_DATE_PATTERN.match(text[:10]):
        return text[:10]
    return None


def reset_date_for_model(model_path: str, created: object) -> str | None:
    return date_from_model_path(model_path) or date_from_created(created)


def reset_dates(df: pd.DataFrame) -> pd.Series:
    if df.empty or "reset.date" not in df.columns:
        return pd.Series(dtype="datetime64[ns]")
    dates = df.loc[df["reset"], "reset.date"].dropna()
    parsed = pd.to_datetime(dates, errors="coerce").dropna().sort_values()
    return parsed


def days_between(dates: pd.Series) -> list[int]:
    if len(dates) < 2:
        return []
    diffs = dates.diff().dt.days.dropna()
    return [int(day) for day in diffs.tolist()]


def pooled_days_between(df: pd.DataFrame, extra_keys: list[str] | None = None) -> list[int]:
    if df.empty:
        return []
    keys = ["workspace.name", "workspace.id"]
    if extra_keys:
        keys.extend(key for key in extra_keys if key not in keys and key in df.columns)
    gaps: list[int] = []
    for _, group in df.groupby(keys, dropna=False):
        gaps.extend(days_between(reset_dates(group)))
    return gaps


def is_causal_only(df: pd.DataFrame) -> bool:
    if df.empty or "model.type" not in df.columns:
        return False
    types = {normalize_model_type(value) for value in df["model.type"].unique()}
    return types == {CAUSAL_TYPE}


def is_conversion_only(df: pd.DataFrame) -> bool:
    if df.empty or "model.type" not in df.columns:
        return False
    types = {normalize_model_type(value) for value in df["model.type"].unique()}
    return types == {CONVERSION_TYPE}


def reset_gaps(df: pd.DataFrame) -> list[int]:
    """Consecutive reset gaps. Pool per workspace; per signal for causal, per objective for conversion."""
    if df.empty:
        return []
    if is_causal_only(df):
        extra_keys = ["model.audience"]
        split_key = "model.audience"
    elif is_conversion_only(df):
        extra_keys = ["model.objective"]
        split_key = "model.objective"
    else:
        extra_keys = []
        split_key = ""
    workspace_ids = df["workspace.id"].nunique(dropna=False)
    split_ids = (
        df[split_key].nunique(dropna=False)
        if split_key and split_key in df.columns
        else 1
    )
    if workspace_ids > 1 or split_ids > 1:
        return pooled_days_between(df, extra_keys)
    return days_between(reset_dates(df))


def gap_stats(gaps: list[int]) -> tuple[str, float | None, float | None]:
    if not gaps:
        return "", None, None
    listed = "[" + ", ".join(str(day) for day in gaps) + "]"
    series = pd.Series(gaps, dtype=float)
    avg = round(float(series.mean()), 2)
    sd = round(float(series.std(ddof=1)), 2) if len(gaps) >= 2 else None
    return listed, avg, sd


def numeric_stats(values: list[float]) -> tuple[str, float | None, float | None]:
    if not values:
        return "", None, None
    listed = "[" + ", ".join(f"{value:.2f}" for value in values) + "]"
    series = pd.Series(values, dtype=float)
    avg = round(float(series.mean()), 2)
    sd = round(float(series.std(ddof=1)), 2) if len(values) >= 2 else None
    return listed, avg, sd


def pct(part: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(100.0 * part / total, 2)


def normalize_label(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "(none)"
    text = str(value).strip()
    return text or "(none)"


def normalize_model_type(value: object) -> str:
    return normalize_label(value)


def normalize_audience(value: object) -> str:
    return normalize_label(value)


def normalize_objective(value: object) -> str:
    return normalize_label(value)


def summary_row(
    scope: str,
    models: int,
    resets: int,
    *,
    workspace_name: str = "",
    workspace_id: str = "",
    model_type: str = "",
    model_audience: str = "",
    signal_name: str = "",
    signal_source: str = "",
    gaps: list[int] | None = None,
) -> dict:
    days_list, days_avg, days_sd = gap_stats(gaps or [])
    return {
        "scope": scope,
        "workspace.name": workspace_name,
        "workspace.id": workspace_id,
        "model.type": model_type,
        "model.audience": model_audience,
        "signal.name": signal_name,
        "signal.source": signal_source,
        "models": models,
        "resets": resets,
        "reset_pct": pct(resets, models),
        "days_between": days_list,
        "days_between_avg": days_avg,
        "days_between_sd": days_sd,
    }


def type_counts(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("model.type", dropna=False)
        .agg(models=("reset", "size"), resets=("reset", "sum"))
        .reset_index()
        .sort_values("model.type")
    )
    grouped["model.type"] = grouped["model.type"].map(normalize_model_type)
    return grouped


def workspace_average_rows(rows: list[dict]) -> list[dict]:
    """Unweighted mean of per-workspace days_between_avg for conversion and causal."""
    avgs_by_type: dict[str, list[float]] = {model_type: [] for model_type in CROSS_WORKSPACE_TYPES}
    workspace_count: dict[str, int] = {model_type: 0 for model_type in CROSS_WORKSPACE_TYPES}

    for row in rows:
        if row["scope"] != "workspace_by_type":
            continue
        model_type = row["model.type"]
        if model_type not in avgs_by_type:
            continue
        workspace_count[model_type] += 1
        avg = row.get("days_between_avg")
        if avg is None or (isinstance(avg, float) and pd.isna(avg)):
            continue
        avgs_by_type[model_type].append(float(avg))

    out: list[dict] = []
    for model_type in CROSS_WORKSPACE_TYPES:
        values = avgs_by_type[model_type]
        listed, mean, sd = numeric_stats(values)
        out.append(
            {
                "scope": "workspaces_avg_by_type",
                "workspace.name": "",
                "workspace.id": "",
                "model.type": model_type,
                "model.audience": "",
                "signal.name": "",
                "signal.source": "",
                "models": workspace_count[model_type],
                "resets": len(values),
                "reset_pct": pct(len(values), workspace_count[model_type]),
                "days_between": listed,
                "days_between_avg": mean,
                "days_between_sd": sd,
            }
        )
    return out


def audience_counts(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("model.audience", dropna=False)
        .agg(models=("reset", "size"), resets=("reset", "sum"))
        .reset_index()
        .sort_values("model.audience")
    )
    grouped["model.audience"] = grouped["model.audience"].map(normalize_audience)
    return grouped


def objective_counts(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("model.objective", dropna=False)
        .agg(models=("reset", "size"), resets=("reset", "sum"))
        .reset_index()
        .sort_values("model.objective")
    )
    grouped["model.objective"] = grouped["model.objective"].map(normalize_objective)
    return grouped


def filter_out_initialization(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """Exclude groups with exactly one reset (treated as model initialization)."""
    if df.empty:
        return df
    missing = [col for col in group_cols if col not in df.columns]
    if missing:
        return df
    group_resets = df.groupby(group_cols, dropna=False)["reset"].transform("sum")
    return df.loc[group_resets != 1].copy()


def aggregation_group_cols(model_type: str) -> list[str]:
    if model_type == CAUSAL_TYPE:
        return ["workspace.name", "workspace.id", "model.audience"]
    return ["workspace.name", "workspace.id", "model.objective"]


def overall_rows(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for model_type in CROSS_WORKSPACE_TYPES:
        type_df = df[df["model.type"] == model_type]
        if type_df.empty:
            continue

        # Single-reset groups are initialization and must not enter aggregates.
        agg_df = filter_out_initialization(type_df, aggregation_group_cols(model_type))
        if agg_df.empty:
            rows.append(
                {
                    "workspace.name": ALL_WORKSPACES,
                    "model.type": model_type,
                    "signal.name": SIGNAL_TOTAL,
                    "signal.source": "",
                    "model.audience": "",
                    "model.objective": "",
                    "models": 0,
                    "resets": 0,
                    "reset_pct": 0.0,
                    "days_between": "",
                    "days_between_avg": None,
                    "days_between_sd": None,
                }
            )
            rows.append(
                {
                    "workspace.name": ALL_WORKSPACES,
                    "model.type": model_type,
                    "signal.name": SIGNAL_WORKSPACE_AVG,
                    "signal.source": "",
                    "model.audience": "",
                    "model.objective": "",
                    "models": 0,
                    "resets": 0,
                    "reset_pct": 0.0,
                    "days_between": "",
                    "days_between_avg": None,
                    "days_between_sd": None,
                }
            )
            continue

        models = len(agg_df)
        resets = int(agg_df["reset"].sum())
        listed, avg, sd = gap_stats(reset_gaps(agg_df))
        rows.append(
            {
                "workspace.name": ALL_WORKSPACES,
                "model.type": model_type,
                "signal.name": SIGNAL_TOTAL,
                "signal.source": "",
                "model.audience": "",
                "model.objective": "",
                "models": models,
                "resets": resets,
                "reset_pct": pct(resets, models),
                "days_between": listed,
                "days_between_avg": avg,
                "days_between_sd": sd,
            }
        )

        workspace_avgs: list[float] = []
        workspace_count = 0
        for _, workspace_df in agg_df.groupby(["workspace.name", "workspace.id"], dropna=False):
            workspace_count += 1
            _listed, workspace_avg, _sd = gap_stats(reset_gaps(workspace_df))
            if workspace_avg is not None:
                workspace_avgs.append(workspace_avg)
        listed_ws, mean_ws, sd_ws = numeric_stats(workspace_avgs)
        rows.append(
            {
                "workspace.name": ALL_WORKSPACES,
                "model.type": model_type,
                "signal.name": SIGNAL_WORKSPACE_AVG,
                "signal.source": "",
                "model.audience": "",
                "model.objective": "",
                "models": workspace_count,
                "resets": len(workspace_avgs),
                "reset_pct": pct(len(workspace_avgs), workspace_count),
                "days_between": listed_ws,
                "days_between_avg": mean_ws,
                "days_between_sd": sd_ws,
            }
        )
    return rows


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    if df.empty:
        return pd.DataFrame(rows, columns=RESULT_COLUMNS)

    # Causal: group by signal (model.audience); conversion: by model.objective.
    causal_df = df[df["model.type"] == CAUSAL_TYPE]
    if not causal_df.empty:
        for (workspace_name, _workspace_id), workspace_df in causal_df.groupby(
            ["workspace.name", "workspace.id"], dropna=False
        ):
            for _, audience_row in audience_counts(workspace_df).iterrows():
                audience_df = workspace_df[
                    workspace_df["model.audience"] == audience_row["model.audience"]
                ]
                gaps = reset_gaps(audience_df)
                listed, avg, sd = gap_stats(gaps)
                rows.append(
                    {
                        "workspace.name": workspace_name,
                        "model.type": CAUSAL_TYPE,
                        "signal.name": first_signal_field(audience_df, "signal.name"),
                        "signal.source": first_signal_field(audience_df, "signal.source"),
                        "model.audience": audience_row["model.audience"],
                        "model.objective": "",
                        "models": int(audience_row["models"]),
                        "resets": int(audience_row["resets"]),
                        "reset_pct": pct(
                            int(audience_row["resets"]), int(audience_row["models"])
                        ),
                        "days_between": listed,
                        "days_between_avg": avg,
                        "days_between_sd": sd,
                    }
                )

    conversion_df = df[df["model.type"] == CONVERSION_TYPE]
    if not conversion_df.empty:
        for (workspace_name, _workspace_id), workspace_df in conversion_df.groupby(
            ["workspace.name", "workspace.id"], dropna=False
        ):
            for _, objective_row in objective_counts(workspace_df).iterrows():
                objective_df = workspace_df[
                    workspace_df["model.objective"] == objective_row["model.objective"]
                ]
                models = int(objective_row["models"])
                resets = int(objective_row["resets"])
                listed, avg, sd = gap_stats(reset_gaps(objective_df))
                rows.append(
                    {
                        "workspace.name": workspace_name,
                        "model.type": CONVERSION_TYPE,
                        "signal.name": "",
                        "signal.source": "",
                        "model.audience": "",
                        "model.objective": objective_row["model.objective"],
                        "models": models,
                        "resets": resets,
                        "reset_pct": pct(resets, models),
                        "days_between": listed,
                        "days_between_avg": avg,
                        "days_between_sd": sd,
                    }
                )

    rows.extend(overall_rows(df))
    results = pd.DataFrame(rows, columns=RESULT_COLUMNS)
    if results.empty:
        return results
    signal_mask = ~results["workspace.name"].eq(ALL_WORKSPACES)
    signal_rows = results.loc[signal_mask].sort_values(
        ["workspace.name", "model.type", "signal.name", "model.audience", "model.objective"],
        kind="stable",
    )
    overall_part = results.loc[~signal_mask]
    parts = [frame for frame in (signal_rows, overall_part) if not frame.empty]
    if len(parts) == 1:
        return parts[0].reset_index(drop=True)
    return pd.concat(parts, ignore_index=True)


def signal_result_label(name: object, source: object, audience_id: object) -> str:
    return f"{name or '(none)'} | {source or '(none)'} | {audience_id}"


def format_gap_suffix(gaps: list[int]) -> str:
    listed, avg, sd = gap_stats(gaps)
    if not listed:
        return " | days between resets: n/a"
    sd_text = "n/a" if sd is None else f"{sd:.2f}"
    return f" | days between resets: {listed} avg={avg:.2f} sd={sd_text}"


def format_result_line(row: pd.Series) -> str:
    listed = row.get("days_between") or ""
    avg = row["days_between_avg"]
    sd = row["days_between_sd"]
    avg_text = "n/a" if avg is None or (isinstance(avg, float) and pd.isna(avg)) else f"{float(avg):.2f}"
    sd_text = "n/a" if sd is None or (isinstance(sd, float) and pd.isna(sd)) else f"{float(sd):.2f}"
    if not listed:
        gap_text = " | days between resets: n/a"
    else:
        gap_text = f" | days between resets: {listed} avg={avg_text} sd={sd_text}"

    signal_name = row.get("signal.name") or ""
    if signal_name == SIGNAL_TOTAL:
        return (
            f"{row['model.type']} | {ALL_WORKSPACES}: "
            f"{int(row['resets'])} / {int(row['models'])} models reset ({float(row['reset_pct']):.2f}%)"
            f"{gap_text}"
        )
    if signal_name == SIGNAL_WORKSPACE_AVG:
        listed_text = listed or "n/a"
        return (
            f"{row['model.type']} | {SIGNAL_WORKSPACE_AVG}: {listed_text} "
            f"avg={avg_text} sd={sd_text} "
            f"(workspaces with avg={int(row['resets'])} / {int(row['models'])})"
        )
    if row["model.type"] == CONVERSION_TYPE:
        return (
            f"{CONVERSION_TYPE} | {row['workspace.name']} | objective {row['model.objective'] or '(none)'}: "
            f"{int(row['resets'])} / {int(row['models'])} models reset ({float(row['reset_pct']):.2f}%)"
            f"{gap_text}"
        )
    return (
        f"{row['model.type']} | {signal_result_label(row['signal.name'], row['signal.source'], row['model.audience'])}: "
        f"{int(row['resets'])} / {int(row['models'])} models reset ({float(row['reset_pct']):.2f}%)"
        f"{gap_text}"
    )


def log_summary(results: pd.DataFrame, logger: logging.Logger) -> None:
    for _, row in results.iterrows():
        logger.info(format_result_line(row))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List models from the last N months and mark whether reset_lstm.json "
            "is True in the targeting S3 path."
        )
    )
    parser.add_argument(
        "--workspace",
        action="append",
        dest="workspaces",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --workspace More",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=6,
        help="Lookback window for model.created (default: 6)",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="ISO date lower bound for model.created (overrides --months)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="CSV output path for the model table",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=None,
        help="CSV output path for reset counts and percentages",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(REPO_ROOT / ".env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logger = logging.getLogger(__name__)

    since = args.since or months_ago(args.months)
    api_url = return_api_url()
    models_url = api_query_url(api_url, "models")
    signals_url = api_query_url(api_url, "signals")
    connections_url = api_query_url(api_url, "connections")
    workspace_filter = normalize_workspace_filter(args.workspaces)
    workspaces = return_workspace_ids(tracking_started=False)

    if workspace_filter:
        workspaces = [
            workspace for workspace in workspaces if workspace["name"] in workspace_filter
        ]
        missing = workspace_filter - {workspace["name"] for workspace in workspaces}
        if missing:
            logger.warning("Workspace(s) not found: %s", ", ".join(sorted(missing)))

    if not workspaces:
        logger.info("No workspaces to process.")
        return

    s3 = S3Connection()
    reset_cache: dict[tuple[str, str], bool] = {}
    rows: list[dict] = []
    api_logger = logging.getLogger("check_model_resets.api")

    for workspace in workspaces:
        workspace_name = workspace["name"]
        account_id = workspace["id"]

        try:
            models = query_all(
                models_url,
                account_id,
                {"created": {"$gte": since}},
                api_logger,
            )
        except Exception as exc:
            logger.exception(
                "Failed querying models for %s (%s): %s", workspace_name, account_id, exc
            )
            continue

        unique_models, _skipped_empty_path = deduplicate_by_path(models)
        signals = query_all_or_empty(signals_url, account_id, {}, api_logger, "signals")
        connections = query_all_or_empty(connections_url, account_id, {}, api_logger, "connections")
        signal_lookup = build_signal_lookup(signals, connections)

        workspace_rows: list[dict] = []
        for model in unique_models:
            model_path = (model.get("path") or "").rstrip("/")
            model_type = normalize_model_type(model.get("type"))
            bucket = targeting_bucket(workspace_name, model_path)
            reset = read_reset_flag(s3, bucket, model_path, reset_cache, logger)
            model_audience = normalize_audience(model.get("audience"))
            model_objective = normalize_objective(model.get("objective"))
            if model_type == CAUSAL_TYPE:
                signal_name, signal_source = signal_fields_for_audience(
                    model_audience, signal_lookup
                )
            else:
                signal_name, signal_source = "", ""
            workspace_rows.append(
                {
                    "workspace.name": workspace_name,
                    "workspace.id": account_id,
                    "model.id": model.get("id"),
                    "model.path": model_path,
                    "model.type": model_type,
                    "model.audience": model_audience,
                    "model.objective": model_objective,
                    "signal.name": signal_name,
                    "signal.source": signal_source,
                    "reset.date": reset_date_for_model(model_path, model.get("created")),
                    "reset": reset,
                }
            )
        rows.extend(workspace_rows)

    df = pd.DataFrame(rows, columns=MODEL_COLUMNS)
    results = summarize(df)

    output_path = args.output or default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    summary_path = args.summary_output or default_summary_path(output_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(summary_path, index=False)

    log_summary(results, logger)


if __name__ == "__main__":
    main()
