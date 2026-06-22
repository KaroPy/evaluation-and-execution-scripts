import json
import os
import re
from datetime import datetime

import awswrangler as wr
import pandas as pd

from general_functions.call_api_with_account_id import (
    make_http_post_call,
    validate_response,
)
from general_functions.conncet_s3 import S3Connection
from general_functions.constants import return_api_url
from general_functions.define_logging import define_logging
from general_functions.return_workspace_ids import return_workspace_ids

PATH = "SprintStories/EN-3218-switch-all-to-audienceSizePerc/"
os.makedirs(PATH, exist_ok=True)
os.makedirs(f"{PATH}logs", exist_ok=True)

MAX_HISTORY_DAYS = 10

logger = define_logging(
    f"{PATH}logs/query_all_audiences-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

url = return_api_url()
workspaces = return_workspace_ids()
s3 = S3Connection()
output_path = f"{PATH}filtered_audiences.csv"


def query_all(endpoint_url: str, account_id: str, content: dict) -> list:
    logger.info(f"Querying {endpoint_url}")
    next_page = 1
    data: list = []
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
        pagination = json_body.get("pagination") or {}
        next_page = pagination.get("next")
    return data


def get_newest_models_by_signal(models: list) -> dict[str, dict]:
    """Return the newest model per signal/audience id (by created timestamp)."""
    newest: dict[str, dict] = {}
    for model in models:
        signal_id = model.get("audience") or model.get("audienceId")
        if not signal_id:
            continue
        existing = newest.get(signal_id)
        if existing is None or model.get("created", "") > existing.get("created", ""):
            newest[signal_id] = model
    return newest


def get_platform_name(connection) -> str | None:
    if not isinstance(connection, dict):
        return None
    platform = connection.get("platform")
    if isinstance(platform, dict):
        return platform.get("name")
    return None


EXCLUDED_NAME_PATTERN = re.compile(r"Premium|Growth|Volume|Visitors", re.IGNORECASE)


def passes_audience_size_filter(name: str | None, audience_size_percentage) -> bool:
    if audience_size_percentage is None or pd.isna(audience_size_percentage):
        return False
    if audience_size_percentage >= 0.5:
        return False
    if name and EXCLUDED_NAME_PATTERN.search(name):
        return False
    return True


def get_targeting_dates(account_id: str, limit: int = MAX_HISTORY_DAYS) -> list[str]:
    prefixes = s3.list_files_with_pagination(
        bucket_name=account_id,
        prefix="targeting.history/",
        delimiter="/",
    )
    dates = [
        p.rstrip("/").split("/")[-1]
        for p in prefixes
        if "meta" not in p and p != "targeting.history/"
    ]
    return sorted(dates, reverse=True)[:limit]


def load_targeting_history(account_id: str, date: str, signal_id: str) -> pd.DataFrame:
    path = f"s3://{account_id}/targeting.history/{date}/{signal_id}.parquet"
    logger.info(f"    Reading {path}")
    try:
        return wr.s3.read_parquet(path, columns=["treatment"])
    except Exception as exc:
        logger.warning(f"    Could not read {path}: {exc}")
        return pd.DataFrame()


def get_treatment_count_dict(targeting_history: pd.DataFrame, signal_id: str) -> dict | None:
    if targeting_history.empty or "treatment" not in targeting_history.columns:
        return None
    counts = targeting_history["treatment"].value_counts()
    return {
        "control": int(counts.get("control", 0)),
        "treatment": int(counts.get(signal_id, 0)),
    }


def enrich_with_targeting_history(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df.empty:
        return result_df

    workspace_dates: dict[str, list[str]] = {}
    for workspace_id in result_df["workspace.id"].unique():
        dates = get_targeting_dates(workspace_id)
        workspace_dates[workspace_id] = dates
        logger.info(f"  Workspace {workspace_id}: {len(dates)} targeting history dates")

    all_dates = sorted(
        {date for dates in workspace_dates.values() for date in dates},
        reverse=True,
    )
    history_cache: dict[tuple[str, str, str], dict | None] = {}

    for date in all_dates:
        result_df[date] = None

    for idx, row in result_df.iterrows():
        workspace_id = row["workspace.id"]
        signal_id = row["signal.id"]
        for date in workspace_dates.get(workspace_id, []):
            cache_key = (workspace_id, signal_id, date)
            if cache_key not in history_cache:
                history = load_targeting_history(workspace_id, date, signal_id)
                history_cache[cache_key] = get_treatment_count_dict(history, signal_id)
            counts = history_cache[cache_key]
            result_df.at[idx, date] = json.dumps(counts) if counts is not None else None

    return result_df


results: list[dict] = []

for workspace in workspaces:
    account_id = workspace["id"]
    account_name = workspace["name"]
    logger.info(f"=== Workspace: {account_name} ({account_id}) ===")

    signals = query_all(f"{url}/signals/query", account_id, {"status": "active"})
    if not signals:
        logger.info("  No active signals — skipping")
        continue
    logger.info(f"  Active signals: {len(signals)}")

    models = query_all(f"{url}/models/query", account_id, {})
    models_by_id = {model["id"]: model for model in models if model.get("id")}
    newest_models = get_newest_models_by_signal(models)
    logger.info(f"  Models loaded: {len(models)}, newest per signal: {len(newest_models)}")

    for signal in signals:
        signal_id = signal.get("id")
        config = signal.get("config") or {}
        model_id = config.get("model")
        model = newest_models.get(signal_id) or (models_by_id.get(model_id, {}) if model_id else {})

        audience_size_percentage = model.get("audienceSizePercentage")
        signal_name = signal.get("name")
        if not passes_audience_size_filter(signal_name, audience_size_percentage):
            continue

        results.append(
            {
                "workspace.id": account_id,
                "workspace.name": account_name,
                "signal.id": signal_id,
                "signal.name": signal_name,
                "signal.connection.platform.name": get_platform_name(signal.get("connection")),
                "signal.type": signal.get("type"),
                "signal.config.purpose": config.get("purpose"),
                "model.id": model.get("id"),
                "model.audienceSizePercentage": audience_size_percentage,
            }
        )

result_df = pd.DataFrame(results)
logger.info(f"Filtered audiences: {len(result_df)} rows")
result_df = enrich_with_targeting_history(result_df)
result_df.to_csv(output_path, index=False)
logger.info(f"Saved {len(result_df)} rows → {output_path}")
