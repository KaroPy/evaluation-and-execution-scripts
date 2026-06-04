import os
from datetime import datetime

import awswrangler as wr
import pandas as pd

from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.conncet_s3 import S3Connection
from general_functions.constants import return_api_url
from general_functions.define_logging import define_logging
from general_functions.return_active_audiences import return_all_audiences
from general_functions.return_workspace_ids import return_workspace_ids

PATH = "SprintStories/EN-3218-switch-all-to-audienceSizePerc/"
os.makedirs(f"{PATH}logs", exist_ok=True)

logger = define_logging(
    f"{PATH}logs/query_missing_audienceSizePerc-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

url = return_api_url()
workspaces = return_workspace_ids()
s3 = S3Connection()

MAX_DATE_FALLBACK = 5


def get_model_by_id(account_id: str, model_id: str) -> dict | None:
    models = call_api_with_accountId(f"{url}/models/query", account_id, {"id": model_id}, logger)
    if not models:
        return None
    return models[0]


def get_targeting_dates(account_id: str) -> list[str]:
    """Return available targeting.history date folders sorted descending."""
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
    return sorted(dates, reverse=True)


def load_targeting_history(
    account_id: str, dates: list[str], audience_id: str
) -> tuple[pd.DataFrame, str | None]:
    """Try dates from most recent, fall back up to MAX_DATE_FALLBACK days."""
    for date in dates[:MAX_DATE_FALLBACK]:
        path = f"s3://{account_id}/targeting.history/{date}/{audience_id}.parquet"
        logger.info(f"    Reading {path}")
        try:
            df = wr.s3.read_parquet(path)
            return df, date
        except Exception as e:
            logger.warning(f"    Could not read {path}: {e}")
    return pd.DataFrame(), None


def calc_audience_size_percentage(df: pd.DataFrame, audience_id: str) -> dict:
    """
    Derive audience size counts and percentage from the treatment column.
    treatment  → audience users
    control    → non-audience users
    """
    counts = df.groupby("treatment")["anonymousId"].nunique()
    count_treatment = int(counts.get(audience_id, 0))
    count_control = int(counts.get("control", 0))
    total = count_treatment + count_control
    percentage = round(count_treatment / total, 6) if total > 0 else None
    logger.info(f"{audience_id}: treatment counts = {counts}, total = {total}")
    return {
        "count_treatment_anonymousIds": count_treatment,
        "count_control_anonymousIds": count_control,
        "percentage": percentage,
    }


results = []
out = f"{PATH}missing_audienceSizePerc_{datetime.now().strftime('%Y%m%d')}.parquet"

for workspace in workspaces:
    account_id = workspace["id"]
    account_name = workspace["name"]
    logger.info(f"=== Workspace: {account_name} ({account_id}) ===")

    # 1. Query all active audiences
    audiences = return_all_audiences(url, account_id, logger)
    if audiences.empty:
        logger.info("  No audiences found — skipping")
        continue
    audiences = audiences[audiences["status"] == "active"]
    if audiences.empty:
        logger.info("  No active audiences — skipping")
        continue
    logger.info(f"  Active audiences: {len(audiences)}")

    # 2. For each audience query the current model (config.model) and filter
    #    for missing audienceSizePercentage
    affected_audiences = []
    for _, audience in audiences.iterrows():
        model_id = audience.get("config.model")
        if not model_id:
            continue
        model = get_model_by_id(account_id, model_id)
        if model is None:
            continue
        if model.get("audienceSizePercentage") is None:
            logger.info(
                f"  Audience {audience['id']} ({audience.get('name')}) "
                f"— model {model_id} missing audienceSizePercentage"
            )
            affected_audiences.append(
                {
                    "id": audience["id"],
                    "name": audience.get("name", ""),
                    "audienceSize": model.get("audienceSize", None),
                }
            )

    logger.info(f"  Audiences missing audienceSizePercentage: {len(affected_audiences)}")
    if not affected_audiences:
        continue

    # 3. Get available targeting history dates once per workspace
    dates = get_targeting_dates(account_id)
    if not dates:
        logger.info("  No targeting history found in S3 — skipping")
        continue
    logger.info(f"  Most recent targeting history date: {dates[0]}")

    # 4. Load targeting history and calculate percentage per audience
    for audience in affected_audiences:
        audience_id = audience["id"]
        audience_name = audience["name"]
        audience_size = audience["audienceSize"]
        logger.info(f"  Processing audience: {audience_name} ({audience_id})")

        df, used_date = load_targeting_history(account_id, dates, audience_id)
        if df.empty:
            logger.warning(f"    No data found in last {MAX_DATE_FALLBACK} days — skipping")
            continue

        logger.info(f"    Loaded {len(df)} rows from date {used_date}")
        stats = calc_audience_size_percentage(df, audience_id)

        results.append(
            {
                "workspace": account_name,
                "audience_name": audience_name,
                "audience_id": audience_id,
                "audienceSize": audience_size,
                "count_control_anonymousIds": stats["count_control_anonymousIds"],
                "count_treatment_anonymousIds": stats["count_treatment_anonymousIds"],
                "percentage": stats["percentage"],
                "targeting_date": used_date,
            }
        )
    result_df = pd.DataFrame(results)
    logger.info(f"\nResult ({len(result_df)} rows):\n{result_df.to_string()}")

    result_df.to_parquet(out)

result_df = pd.DataFrame(results)
logger.info(f"\nResult ({len(result_df)} rows):\n{result_df.to_string()}")

result_df.to_parquet(out)
logger.info(f"Saved → {out}")
