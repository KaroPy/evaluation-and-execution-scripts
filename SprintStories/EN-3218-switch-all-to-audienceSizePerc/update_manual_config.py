import json
import os
from datetime import datetime, timezone

from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.constants import return_api_url
from general_functions.define_logging import define_logging
from general_functions.return_workspace_ids import return_workspace_ids

PATH = "SprintStories/EN-3218-switch-all-to-audienceSizePerc/"
os.makedirs(f"{PATH}logs", exist_ok=True)

logger = define_logging(
    f"{PATH}logs/update_manual_config-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

url = return_api_url()
CONFIG_PATH = f"{PATH}manual_config_changes.json"

DRY_RUN: bool = False


def utc_now_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def get_signal(account_id: str, signal_id: str) -> dict | None:
    result = call_api_with_accountId(f"{url}/signals/query", account_id, {"id": signal_id}, logger)
    if not result:
        return None
    return result[0]


def get_model(account_id: str, model_id: str) -> dict | None:
    result = call_api_with_accountId(f"{url}/models/query", account_id, {"id": model_id}, logger)
    if not result:
        return None
    return result[0]


def store_model(account_id: str, model: dict, signal: dict) -> str | None:
    payload = model.copy()
    payload.pop("id", None)
    payload["created"] = utc_now_iso()
    if not payload.get("scope"):
        treatments = (signal.get("config") or {}).get("treatments") or {}
        payload["scope"] = treatments.get("scope")
    logger.info(f"  models/store payload: {payload}")
    result = call_api_with_accountId(f"{url}/models/store", account_id, payload, logger)
    if not result:
        return None
    return result.get("id") if isinstance(result, dict) else result["id"]


def update_signal_model(account_id: str, signal_id: str, model_id: str) -> None:
    call_api_with_accountId(
        f"{url}/signals/update",
        account_id,
        {"id": signal_id, "model": model_id},
        logger,
    )


def load_manual_config(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


workspace_lookup = {w["name"]: w["id"] for w in return_workspace_ids()}
manual_config = load_manual_config(CONFIG_PATH)
logger.info(f"Loaded manual config from {CONFIG_PATH}")

for workspace_name, signal_changes in manual_config.items():
    account_id = workspace_lookup.get(workspace_name)
    if not account_id:
        logger.warning(f"No account_id found for workspace '{workspace_name}' — skipping")
        continue

    logger.info(f"=== Workspace: {workspace_name} ({account_id}) ===")

    for signal_id, changes in signal_changes.items():
        logger.info(f"--- Signal {signal_id} ---")

        signal = get_signal(account_id, signal_id)
        if signal is None:
            logger.warning("  Signal not found — skipping")
            continue

        model_id = signal.get("model")
        if not model_id:
            logger.warning("  No model set on signal — skipping")
            continue

        model = get_model(account_id, model_id)
        if model is None:
            logger.warning(f"  Model {model_id} not found — skipping")
            continue

        logger.info(f"  Current model: {model_id}")
        logger.info(f"  Current audienceSizePercentage: {model.get('audienceSizePercentage')}")

        for field, value in changes.items():
            model[field] = value
            logger.info(f"  Setting {field}={value}")

        if DRY_RUN:
            logger.info("  [DRY RUN] Would call models/store — skipping")
            logger.info(f"  [DRY RUN] Would call signals/update with model=test-dry-run — skipping")
            continue

        new_model_id = store_model(account_id, model, signal)
        if not new_model_id:
            logger.warning("  models/store returned no id — skipping signal update")
            continue

        logger.info(f"  New model: {new_model_id}")
        update_signal_model(account_id, signal_id, new_model_id)
        logger.info(f"  Signal updated → model {new_model_id}")
