"""
Extract all signals and objectives without an active signal per workspace.

For each workspace:
  1. Query all signals (signals/query)
  2. Query all objectives (objectives/query)
  3. Write a signals extract
  4. Write objectives that are not linked to any active signal via signal.objective

Usage (from repo root):
    python Analysis/SignalInsights/extract_signals_and_objectives_without_active_signal.py
    python Analysis/SignalInsights/extract_signals_and_objectives_without_active_signal.py --customer Pendix
    python Analysis/SignalInsights/extract_signals_and_objectives_without_active_signal.py \\
        --signals-output Analysis/SignalInsights/data/signals.csv \\
        --objectives-output Analysis/SignalInsights/data/objectives_without_active_signal.csv
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
    make_http_post_call,
    validate_response,
)
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_SIGNALS_OUTPUT = DATA_DIR / "signals_extract.csv"
DEFAULT_OBJECTIVES_OUTPUT = DATA_DIR / "objectives_without_active_signal.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"extract_signals_objectives_{stamp}.log"


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


def normalize_customer_filter(customers: list[str] | None) -> set[str] | None:
    if not customers:
        return None
    names: set[str] = set()
    for item in customers:
        for part in item.split(","):
            name = part.strip()
            if name:
                names.add(name)
    return names or None


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


def normalize_events(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item)]
    text = str(value).strip()
    return [text] if text else []


def extract_signal_rows(
    workspace_name: str,
    workspace_id: str,
    signals: list[dict],
) -> list[dict]:
    rows: list[dict] = []
    for signal in signals:
        config = signal.get("config") or {}
        rows.append(
            {
                "workspace.name": workspace_name,
                "workspace.id": workspace_id,
                "signal.id": signal.get("id"),
                "signal.name": signal.get("name"),
                "signal.status": signal.get("status"),
                "signal.type": signal.get("type"),
                "connection.platform.name": get_platform_name(signal),
                "config.purpose": config.get("purpose"),
                "signal.objective": signal.get("objective"),
                "signal.model": signal.get("model"),
                "signal.createdAt": signal.get("createdAt"),
                "signal.updatedAt": signal.get("updatedAt"),
            }
        )
    return rows


def count_signals_by_objective(signals: list[dict]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for signal in signals:
        objective_id = signal.get("objective")
        if objective_id is None:
            continue
        objective_key = str(objective_id)
        bucket = counts.setdefault(
            objective_key,
            {"active_signal_count": 0, "inactive_signal_count": 0, "total_signal_count": 0},
        )
        bucket["total_signal_count"] += 1
        if signal.get("status") == "active":
            bucket["active_signal_count"] += 1
        else:
            bucket["inactive_signal_count"] += 1
    return counts


def extract_objectives_without_active_signal(
    workspace_name: str,
    workspace_id: str,
    objectives: list[dict],
    signals: list[dict],
) -> list[dict]:
    signal_counts = count_signals_by_objective(signals)
    rows: list[dict] = []

    for objective in objectives:
        objective_id = objective.get("id")
        if objective_id is None:
            continue

        objective_key = str(objective_id)
        counts = signal_counts.get(
            objective_key,
            {
                "active_signal_count": 0,
                "inactive_signal_count": 0,
                "total_signal_count": 0,
            },
        )
        if counts["active_signal_count"] > 0:
            continue

        rows.append(
            {
                "workspace.name": workspace_name,
                "workspace.id": workspace_id,
                "objective.id": objective_id,
                "objective.name": objective.get("name"),
                "objective.events": normalize_events(objective.get("events")),
                "active_signal_count": counts["active_signal_count"],
                "inactive_signal_count": counts["inactive_signal_count"],
                "total_signal_count": counts["total_signal_count"],
            }
        )

    return rows


def process_workspace(
    api_url: str,
    workspace: dict,
    logger: logging.Logger,
) -> tuple[list[dict], list[dict]]:
    account_id = workspace["id"]
    workspace_name = workspace["name"]

    signals = query_all(f"{api_url}/api/signals/query", account_id, {}, logger)
    objectives = query_all(f"{api_url}/api/objectives/query", account_id, {}, logger)

    signal_rows = extract_signal_rows(workspace_name, account_id, signals)
    objective_rows = extract_objectives_without_active_signal(
        workspace_name,
        account_id,
        objectives,
        signals,
    )

    logger.info(
        "Workspace %s: %s signals, %s objectives, %s objectives without active signal",
        workspace_name,
        len(signal_rows),
        len(objectives),
        len(objective_rows),
    )
    return signal_rows, objective_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract all signals and objectives without an active signal "
            "from the targeting API."
        )
    )
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --customer Pendix",
    )
    parser.add_argument(
        "--signals-output",
        type=Path,
        default=DEFAULT_SIGNALS_OUTPUT,
        help=f"CSV output for all signals (default: {DEFAULT_SIGNALS_OUTPUT.name})",
    )
    parser.add_argument(
        "--objectives-output",
        type=Path,
        default=DEFAULT_OBJECTIVES_OUTPUT,
        help=(
            "CSV output for objectives without an active signal "
            f"(default: {DEFAULT_OBJECTIVES_OUTPUT.name})"
        ),
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
    customer_filter = normalize_customer_filter(args.customers)
    workspaces = return_workspace_ids(tracking_started=False)

    if customer_filter:
        workspaces = [
            workspace for workspace in workspaces if workspace["name"] in customer_filter
        ]
        missing = customer_filter - {workspace["name"] for workspace in workspaces}
        if missing:
            logger.warning("Workspace(s) not found: %s", ", ".join(sorted(missing)))

    if not workspaces:
        logger.info("No workspaces to process.")
        return

    all_signal_rows: list[dict] = []
    all_objective_rows: list[dict] = []

    for workspace in workspaces:
        logger.info("=== Workspace: %s (%s) ===", workspace["name"], workspace["id"])
        signal_rows, objective_rows = process_workspace(api_url, workspace, logger)
        all_signal_rows.extend(signal_rows)
        all_objective_rows.extend(objective_rows)

    args.signals_output.parent.mkdir(parents=True, exist_ok=True)
    args.objectives_output.parent.mkdir(parents=True, exist_ok=True)

    signals_df = pd.DataFrame(all_signal_rows)
    objectives_df = pd.DataFrame(all_objective_rows)

    signals_df.to_csv(args.signals_output, index=False)
    objectives_df.to_csv(args.objectives_output, index=False)

    logger.info("Wrote %s signals to %s", len(signals_df), args.signals_output.resolve())
    logger.info(
        "Wrote %s objectives without active signal to %s",
        len(objectives_df),
        args.objectives_output.resolve(),
    )


if __name__ == "__main__":
    main()
