import os
from datetime import datetime, timezone

import pandas as pd

from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.constants import return_api_url
from general_functions.define_logging import define_logging
from general_functions.return_workspace_ids import return_workspace_ids

PATH = "SprintStories/EN-3218-switch-all-to-audienceSizePerc/"
os.makedirs(f"{PATH}logs", exist_ok=True)

logger = define_logging(
    f"{PATH}logs/update_audienceSizePerc-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

url = return_api_url()

# --- configure which workspaces to process -----------------------------------
DESIRED_WORKSPACES: list[str] = [
    "to teach"
    # "workspace-name-1",
    # "workspace-name-2",
]
# -----------------------------------------------------------------------------

INPUT_FILE = f"{PATH}missing_audienceSizePerc_20260527.parquet"


def get_audience(account_id: str, audience_id: str) -> dict | None:
    result = call_api_with_accountId(
        f"{url}/audiences/query", account_id, {"id": audience_id}, logger
    )
    if not result:
        return None
    return result[0]


def get_model(account_id: str, model_id: str) -> dict | None:
    result = call_api_with_accountId(f"{url}/models/query", account_id, {"id": model_id}, logger)
    if not result:
        return None
    return result[0]


def store_model(account_id: str, model: dict, scope: str) -> str | None:
    """Post a new model (without id) and return the new model id."""
    if scope == "campaignBased":
        scope = "campaign"
    else:
        raise ValueError(f"Unknown scope: {scope}")
    model["scope"] = scope
    payload = model.copy()
    payload.pop("id", None)
    now = datetime.now(timezone.utc)
    payload["created"] = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    logger.info(f"Update model with payload: {payload}")
    result = call_api_with_accountId(f"{url}/models/store", account_id, payload, logger)
    logger.info(f"Updated model: {result}")
    if not result:
        return None
    return result["id"]


def update_audience_model(account_id: str, audience_id: str, model_id: str) -> None:
    call_api_with_accountId(
        f"{url}/audiences/update",
        account_id,
        {"id": audience_id, "config": {"model": model_id}},
        logger,
    )


# Build workspace name → id lookup
workspace_lookup = {w["name"]: w["id"] for w in return_workspace_ids()}

df = pd.read_parquet(INPUT_FILE)
logger.info(f"Loaded {len(df)} rows from {INPUT_FILE}")

if DESIRED_WORKSPACES:
    df = df[df["workspace"].isin(DESIRED_WORKSPACES)]
    logger.info(f"Filtered to {len(df)} rows for workspaces: {DESIRED_WORKSPACES}")

for _, row in df.iterrows():
    workspace_name = row["workspace"]
    audience_id = row["audience_id"]
    audience_name = row["audience_name"]
    new_percentage = round(row["percentage"], 0)

    account_id = workspace_lookup.get(workspace_name)
    if not account_id:
        logger.warning(f"No account_id found for workspace '{workspace_name}' — skipping")
        continue

    logger.info(f"=== {workspace_name} / {audience_name} ({audience_id}) ===")

    # 1. Query audience to get the current model id
    audience = get_audience(account_id, audience_id)
    if audience is None:
        logger.warning("  Audience not found — skipping")
        continue

    model_id = audience.get("config", {}).get("model")
    if not model_id:
        logger.warning("  No model set on audience — skipping")

    scope = audience.get("config", {}).get("treatmentSyncStrategy", None)

    # 2. Query the current model
    model = get_model(account_id, model_id)
    if model is None:
        logger.warning(f"  Model {model_id} not found — skipping")
        continue

    logger.info(f"  Current model: {model_id}")

    # 3. Build updated model: clear audienceSize, set audienceSizePercentage
    model["audienceSize"] = None
    model["audienceSizePercentage"] = new_percentage
    logger.info(f"  Setting audienceSizePercentage={new_percentage}, audienceSize=None")

    # 4. Store as new model
    new_model_id = store_model(account_id, model, scope)
    if not new_model_id:
        logger.warning("  models/store returned no id — skipping audience update")
        continue

    logger.info(f"  New model stored: {new_model_id}")

    # 5. Point the audience at the new model
    update_audience_model(account_id, audience_id, new_model_id)
    logger.info(f"  Audience updated → model {new_model_id}")
