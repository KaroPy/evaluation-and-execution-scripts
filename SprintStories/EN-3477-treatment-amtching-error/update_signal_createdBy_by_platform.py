"""
Update signal.createdBy for all signals on a given connection.platform.name.

For a workspace:
  1. Query all signals (signals/query)
  2. Filter by connection.platform.name
  3. Update each matching signal via signals/update with
     {"id": signal.id, "createdBy": createdBy_id}

Dry-run is the default. Pass --apply to write changes.

Usage (from repo root):
    python SprintStories/EN-3477-treatment-amtching-error/update_signal_createdBy_by_platform.py \\
        --customer More --platform facebook --created-by 6a0d7318ab9c08ffd3e721d3
    python SprintStories/EN-3477-treatment-amtching-error/update_signal_createdBy_by_platform.py \\
        --customer More --platform facebook --created-by 6a0d7318ab9c08ffd3e721d3 --apply
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


def default_plan_output() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"update_signal_createdBy_plan_{stamp}.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"update_signal_createdBy_{stamp}.log"


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


def query_all(
    endpoint_url: str,
    account_id: str,
    content: dict,
    logger: logging.Logger,
) -> list[dict]:
    logger.info("Querying %s", endpoint_url)
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
    logger.info("Fetched %s elements", len(data))
    return data


def get_platform_name(signal: dict) -> str | None:
    connection = signal.get("connection") or {}
    platform = connection.get("platform") or {}
    name = platform.get("name")
    return str(name) if name is not None else None


def get_created_by_id(signal: dict) -> str | None:
    created_by = signal.get("createdBy")
    if created_by is None or created_by == "":
        return None
    if isinstance(created_by, dict):
        user_id = created_by.get("id")
        return str(user_id) if user_id else None
    return str(created_by)


def resolve_workspace(customer: str, logger: logging.Logger) -> dict:
    workspaces = return_workspace_ids(tracking_started=False)
    matches = [ws for ws in workspaces if ws["name"] == customer]
    if not matches:
        known = ", ".join(sorted(ws["name"] for ws in workspaces))
        raise SystemExit(
            f"Workspace '{customer}' not found. Known workspaces: {known}"
        )
    if len(matches) > 1:
        logger.warning(
            "Multiple workspaces named %s; using the first (%s)",
            customer,
            matches[0]["id"],
        )
    return matches[0]


def build_plans(
    signals: list[dict],
    workspace: dict,
    platform_name: str,
    target_created_by: str,
) -> list[dict]:
    plans: list[dict] = []
    for signal in signals:
        signal_platform = get_platform_name(signal)
        if signal_platform != platform_name:
            continue

        signal_id = signal.get("id")
        current_created_by = get_created_by_id(signal)
        already_set = current_created_by == target_created_by

        plans.append(
            {
                "workspace.name": workspace["name"],
                "workspace.id": workspace["id"],
                "signal.id": signal_id,
                "signal.name": signal.get("name"),
                "signal.status": signal.get("status"),
                "connection.platform.name": signal_platform,
                "createdBy.current": current_created_by,
                "createdBy.target": target_created_by,
                "action": "skip" if already_set else "update",
                "reason": (
                    "createdBy already matches target"
                    if already_set
                    else "createdBy differs from target"
                ),
            }
        )
    return plans


def update_signal(
    api_url: str,
    account_id: str,
    signal_id: str,
    created_by_id: str,
    logger: logging.Logger,
) -> None:
    payload = {"id": signal_id, "createdBy": created_by_id}
    call_api_with_accountId(
        f"{api_url}/api/signals/update",
        account_id,
        payload,
        logger,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Set signal.createdBy for all signals matching a "
            "connection.platform.name in one workspace."
        )
    )
    parser.add_argument(
        "--customer",
        required=True,
        metavar="NAME",
        help="Workspace name, e.g. --customer More",
    )
    parser.add_argument(
        "--platform",
        required=True,
        metavar="NAME",
        help="connection.platform.name to filter, e.g. --platform facebook",
    )
    parser.add_argument(
        "--created-by",
        required=True,
        dest="created_by",
        metavar="ID",
        help="Target createdBy user id to set on matching signals",
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
        "--output",
        type=Path,
        default=None,
        help="CSV plan output path",
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
    logger.info("Mode: %s", "APPLY" if args.apply else "DRY-RUN")

    api_url = return_api_url().rstrip("/")
    workspace = resolve_workspace(args.customer, logger)
    account_id = workspace["id"]

    logger.info(
        "Workspace %s (%s) | platform=%s | createdBy=%s",
        workspace["name"],
        account_id,
        args.platform,
        args.created_by,
    )

    signals = query_all(f"{api_url}/api/signals/query", account_id, {}, logger)
    plans = build_plans(signals, workspace, args.platform, args.created_by)
    df = pd.DataFrame(plans)

    output_path = args.output or default_plan_output()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Wrote plan (%s rows) to %s", len(df), output_path.resolve())

    if df.empty:
        logger.warning(
            "No signals found for platform '%s' in workspace '%s'.",
            args.platform,
            workspace["name"],
        )
        return

    action_counts = df["action"].value_counts().to_dict()
    logger.info("Planned actions: %s", action_counts)

    to_update = df[df["action"] == "update"]
    for _, row in to_update.iterrows():
        logger.info(
            "%s | %s | %s | createdBy %s -> %s",
            row["workspace.name"],
            row["signal.id"],
            row["signal.name"],
            row["createdBy.current"],
            row["createdBy.target"],
        )

    if not args.apply:
        logger.info(
            "Dry-run only. Re-run with --apply to update %s signal(s).",
            len(to_update),
        )
        return

    updated = 0
    for _, row in to_update.iterrows():
        signal_id = row["signal.id"]
        logger.info(
            "Updating signal %s with createdBy=%s",
            signal_id,
            args.created_by,
        )
        update_signal(api_url, account_id, signal_id, args.created_by, logger)
        updated += 1

    logger.info("Updated %s signal(s).", updated)


if __name__ == "__main__":
    main()
