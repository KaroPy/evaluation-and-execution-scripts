"""
Fill signal.config.size from the signal model's audienceSizePercentage.

For each active signal:
  1. Load the configured model (signal.config.model)
  2. Read model.audienceSizePercentage
  3. Update the signal via signals/update with {"config": {"size": <value>}}

Use --dry-run to preview changes (default) or --apply to write to production.

Usage (from repo root):
    python SprintStories/EN-3370-fill-signal-size-property/fill_signal_size_property.py
    python SprintStories/EN-3370-fill-signal-size-property/fill_signal_size_property.py --workspace Rosental
    python SprintStories/EN-3370-fill-signal-size-property/fill_signal_size_property.py --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.call_api_with_account_id import (  # noqa: E402
    call_api_with_accountId,
    make_http_post_call,
    validate_response,
)
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_PLAN_OUTPUT = DATA_DIR / "fill_signal_size_property_plan.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"fill_signal_size_property_{stamp}.log"


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


def query_all(endpoint_url: str, account_id: str, content: dict, logger: logging.Logger) -> list:
    logger.info("Querying %s", endpoint_url)
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


def get_model(account_id: str, model_id: str, api_url: str, logger: logging.Logger) -> dict | None:
    models = call_api_with_accountId(
        f"{api_url}/api/models/query",
        account_id,
        {"id": model_id},
        logger,
    )
    return models[0] if models else None


def build_signal_update_payload(signal_id: str, size: float | int) -> dict:
    return {
        "id": signal_id,
        "config": {
            "size": size,
        },
    }


def values_equal(left, right) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def build_workspace_plans(
    api_url: str,
    workspace: dict,
    logger: logging.Logger,
) -> list[dict]:
    account_id = workspace["id"]
    workspace_name = workspace["name"]
    plans: list[dict] = []

    signals = query_all(f"{api_url}/api/signals/query", account_id, {"status": "active"}, logger)
    logger.info("Workspace %s: %s active signals", workspace_name, len(signals))

    for signal in signals:
        signal_id = signal.get("id")
        signal_name = signal.get("name")
        config = signal.get("config") or {}
        current_size = config.get("size")
        model_id = signal.get("model")

        plan = {
            "workspace.name": workspace_name,
            "workspace.id": account_id,
            "signal.id": signal_id,
            "signal.name": signal_name,
            "signal.status": signal.get("status"),
            "model.id": model_id,
            "model.audienceSizePercentage": None,
            "signal.config.size.current": current_size,
            "signal.config.size.target": None,
            "action": "skip",
            "reason": "",
        }

        if not model_id:
            plan["reason"] = "no config.model on signal"
            plans.append(plan)
            continue

        model = get_model(account_id, model_id, api_url, logger)
        if model is None:
            plan["reason"] = f"model {model_id} not found"
            plans.append(plan)
            continue

        audience_size_percentage = model.get("audienceSizePercentage")
        plan["model.audienceSizePercentage"] = audience_size_percentage
        plan["signal.config.size.target"] = audience_size_percentage

        if audience_size_percentage is None:
            plan["reason"] = "model.audienceSizePercentage is missing"
            plans.append(plan)
            continue

        if values_equal(current_size, audience_size_percentage):
            plan["reason"] = "signal.config.size already matches model"
            plans.append(plan)
            continue

        plan["action"] = "update"
        plan["reason"] = "size differs from model.audienceSizePercentage"
        plans.append(plan)

    return plans


def update_signal(
    api_url: str,
    account_id: str,
    payload: dict,
    logger: logging.Logger,
) -> None:
    call_api_with_accountId(
        f"{api_url}/api/signals/update",
        account_id,
        payload,
        logger,
    )


def print_plan_summary(plans: pd.DataFrame) -> None:
    action_counts = plans["action"].value_counts().to_dict()
    logging.info("Planned actions: %s", action_counts)
    to_update = plans[plans["action"] == "update"]
    logging.info("Signals to update: %s", len(to_update))

    for _, row in to_update.iterrows():
        logging.info(
            "%s | %s | %s | size %s -> %s (model %s)",
            row["workspace.name"],
            row["signal.id"],
            row["signal.name"],
            row["signal.config.size.current"],
            row["signal.config.size.target"],
            row["model.id"],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fill signal.config.size from model.audienceSizePercentage."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_false",
        dest="apply",
        help="Preview planned changes without writing to the API (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        dest="apply",
        help="Apply changes via signals/update",
    )
    parser.set_defaults(apply=False)
    parser.add_argument(
        "--workspace",
        action="append",
        dest="workspaces",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --workspace Rosental",
    )
    parser.add_argument(
        "--plan-output",
        type=Path,
        default=DEFAULT_PLAN_OUTPUT,
        help=f"CSV plan output path (default: {DEFAULT_PLAN_OUTPUT.name})",
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
    logger.info("Logging to %s", log_path)

    api_url = return_api_url().rstrip("/")
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

    all_plans: list[dict] = []
    for workspace in workspaces:
        logger.info("=== Workspace: %s (%s) ===", workspace["name"], workspace["id"])
        all_plans.extend(build_workspace_plans(api_url, workspace, logger))

    plans = pd.DataFrame(all_plans)
    args.plan_output.parent.mkdir(parents=True, exist_ok=True)
    plans.to_csv(args.plan_output, index=False)
    logger.info("Saved plan to %s", args.plan_output)

    if plans.empty:
        logger.info("No active signals found.")
        return

    print_plan_summary(plans)

    to_update = plans[plans["action"] == "update"]
    if to_update.empty:
        logger.info("Nothing to update.")
        return

    updated = 0
    for _, row in to_update.iterrows():
        payload = build_signal_update_payload(
            row["signal.id"],
            row["signal.config.size.target"],
        )
        logger.info(
            "Updating %s | %s | config.size=%s",
            row["workspace.name"],
            row["signal.id"],
            row["signal.config.size.target"],
        )
        if not args.apply:
            logging.info(f"Payload: {payload}")
            logger.info("Dry run only. Re-run with --apply to execute %s updates.", len(to_update))
            updated += 1
            continue
        update_signal(api_url, row["workspace.id"], payload, logger)
        updated += 1

    logger.info("Updated %s signals.", updated)


if __name__ == "__main__":
    main()
