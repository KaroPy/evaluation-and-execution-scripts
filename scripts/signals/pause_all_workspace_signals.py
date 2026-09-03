"""
Pause all signals in a specific workspace.

For the given workspace:
  1. Query all signals (signals/query)
  2. Set status to "paused" via signals/update for every signal that is not
     already paused

Dry-run is the default. Pass --apply to write changes.

Usage (from repo root):
    python scripts/signals/pause_all_workspace_signals.py --workspace Rosental
    python scripts/signals/pause_all_workspace_signals.py --workspace Rosental --apply
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

TARGET_STATUS = "paused"


def default_plan_output() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"pause_all_workspace_signals_plan_{stamp}.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"pause_all_workspace_signals_{stamp}.log"


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


def build_plan(
    workspace_name: str,
    account_id: str,
    signals: list[dict],
) -> list[dict]:
    plans: list[dict] = []
    for signal in signals:
        signal_id = signal.get("id")
        current_status = signal.get("status")
        already_paused = current_status == TARGET_STATUS
        plans.append(
            {
                "workspace.name": workspace_name,
                "workspace.id": account_id,
                "signal.id": signal_id,
                "signal.name": signal.get("name"),
                "signal.type": signal.get("type"),
                "signal.status.current": current_status,
                "signal.status.target": TARGET_STATUS,
                "action": "skip" if already_paused else "pause",
                "reason": (
                    "already paused"
                    if already_paused
                    else f"set status {current_status!r} -> {TARGET_STATUS!r}"
                ),
            }
        )
    return plans


def update_signal_status(
    api_url: str,
    account_id: str,
    signal_id: str,
    logger: logging.Logger,
) -> None:
    call_api_with_accountId(
        f"{api_url}/api/signals/update",
        account_id,
        {"id": signal_id, "status": TARGET_STATUS},
        logger,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pause all signals in a specific workspace."
    )
    parser.add_argument(
        "--workspace",
        required=True,
        metavar="NAME",
        help="Workspace name to pause signals in, e.g. --workspace Rosental",
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
        help="Apply status=paused via signals/update",
    )
    parser.set_defaults(apply=False)
    parser.add_argument(
        "--plan-output",
        type=Path,
        default=None,
        help="CSV plan output path (default: data/pause_all_workspace_signals_plan_<timestamp>.csv)",
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
    workspaces = return_workspace_ids(tracking_started=False)
    matches = [ws for ws in workspaces if ws["name"] == args.workspace]

    if not matches:
        available = ", ".join(sorted(ws["name"] for ws in workspaces))
        raise SystemExit(
            f"Workspace {args.workspace!r} not found. Available: {available}"
        )
    if len(matches) > 1:
        raise SystemExit(
            f"Multiple workspaces named {args.workspace!r}: {[ws['id'] for ws in matches]}"
        )

    workspace = matches[0]
    account_id = workspace["id"]
    workspace_name = workspace["name"]
    logger.info("=== Workspace: %s (%s) ===", workspace_name, account_id)

    signals = query_all(f"{api_url}/api/signals/query", account_id, {}, logger)
    plans = build_plan(workspace_name, account_id, signals)
    plans_df = pd.DataFrame(plans)

    plan_output = args.plan_output or default_plan_output()
    plan_output.parent.mkdir(parents=True, exist_ok=True)
    plans_df.to_csv(plan_output, index=False)
    logger.info("Saved plan to %s", plan_output)

    if plans_df.empty:
        logger.info("No signals found.")
        return

    action_counts = plans_df["action"].value_counts().to_dict()
    logger.info("Planned actions: %s", action_counts)

    to_pause = plans_df[plans_df["action"] == "pause"]
    for _, row in to_pause.iterrows():
        logger.info(
            "  %s | %s | %s -> %s",
            row["signal.id"],
            row["signal.name"],
            row["signal.status.current"],
            row["signal.status.target"],
        )

    if to_pause.empty:
        logger.info("Nothing to update — all signals already paused.")
        return

    updated = 0
    for _, row in to_pause.iterrows():
        signal_id = row["signal.id"]
        payload = {"id": signal_id, "status": TARGET_STATUS}
        if not args.apply:
            logger.info("[DRY-RUN] Would call signals/update with %s", payload)
            updated += 1
            continue

        logger.info("Updating %s | status=%s", signal_id, TARGET_STATUS)
        update_signal_status(api_url, account_id, signal_id, logger)
        updated += 1

    if args.apply:
        logger.info("Paused %s signals.", updated)
    else:
        logger.info(
            "Dry run only. Re-run with --apply to pause %s signals.",
            updated,
        )


if __name__ == "__main__":
    main()
