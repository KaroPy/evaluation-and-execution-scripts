"""
Find platforms (sources) with multiple distinct signal.createdBy values.

For each workspace:
  1. Query all signals (signals/query)
  2. Group by connection.platform.id
  3. Count unique createdBy ids
  4. Resolve emails via core/user/internal/query

Output rows (platforms with more than one createdBy) include:
  workspace.name, connection.platform.name,
  createdBy {createdById: {"email": mail, "signals": count}}

Usage (from repo root):
    python Analysis/SignalInsights/extract_signals_find_multiple_createdBy_per_source.py
    python Analysis/SignalInsights/extract_signals_find_multiple_createdBy_per_source.py --customer "to teach"
    python Analysis/SignalInsights/extract_signals_find_multiple_createdBy_per_source.py \\
        --output Analysis/SignalInsights/data/multiple_createdBy_per_source.json
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


def default_output_paths() -> tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"multiple_createdBy_per_source_{stamp}"
    return DATA_DIR / f"{stem}.json", DATA_DIR / f"{stem}.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"multiple_createdBy_per_source_{stamp}.log"


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


def get_platform(signal: dict) -> tuple[str | None, str | None]:
    connection = signal.get("connection") or {}
    platform = connection.get("platform") or {}
    platform_id = platform.get("id")
    platform_name = platform.get("name")
    return (
        str(platform_id) if platform_id is not None else None,
        str(platform_name) if platform_name is not None else None,
    )


def get_created_by_id(signal: dict) -> str | None:
    created_by = signal.get("createdBy")
    if created_by is None or created_by == "":
        return None
    if isinstance(created_by, dict):
        user_id = created_by.get("id")
        return str(user_id) if user_id else None
    return str(created_by)


def query_user_email(
    api_url: str,
    account_id: str,
    user_id: str,
    logger: logging.Logger,
    cache: dict[str, str | None],
) -> str | None:
    if user_id in cache:
        return cache[user_id]

    endpoint = f"{api_url}/api/core/user/internal/query"
    payload = json.dumps(
        {
            "content": {"id": user_id},
            "pagination": {"page": 1},
            "context": {"accountId": account_id},
        }
    )
    try:
        json_body = make_http_post_call(endpoint, payload, logger, return_error=True)
        data = json_body.get("data") or []
        email = None
        if isinstance(data, list) and data:
            email = data[0].get("email")
        elif isinstance(data, dict):
            email = data.get("email")
        cache[user_id] = str(email) if email else None
    except Exception as exc:
        logger.warning("Could not resolve user %s: %s", user_id, exc)
        cache[user_id] = None

    return cache[user_id]


def group_created_by_per_platform(
    signals: list[dict],
) -> dict[str, dict]:
    """
    Group signals by platform id.

    Returns:
        platform_id -> {
            "connection.platform.name": str | None,
            "createdBySignalIds": {createdById: set[signalId]},
        }
    """
    grouped: dict[str, dict] = {}
    for signal in signals:
        platform_id, platform_name = get_platform(signal)
        if not platform_id:
            continue
        bucket = grouped.setdefault(
            platform_id,
            {
                "connection.platform.name": platform_name,
                "createdBySignalIds": {},
            },
        )
        if platform_name and not bucket["connection.platform.name"]:
            bucket["connection.platform.name"] = platform_name
        created_by_id = get_created_by_id(signal)
        signal_id = signal.get("id")
        if not created_by_id or not signal_id:
            continue
        bucket["createdBySignalIds"].setdefault(created_by_id, set()).add(str(signal_id))
    return grouped


def process_workspace(
    api_url: str,
    workspace: dict,
    logger: logging.Logger,
    user_email_cache: dict[str, str | None],
    *,
    min_created_by: int = 2,
) -> list[dict]:
    account_id = workspace["id"]
    workspace_name = workspace["name"]

    signals = query_all(f"{api_url}/api/signals/query", account_id, {}, logger)
    grouped = group_created_by_per_platform(signals)

    rows: list[dict] = []
    for platform_id, bucket in grouped.items():
        signal_ids_by_user: dict[str, set[str]] = bucket["createdBySignalIds"]
        created_by_ids = sorted(signal_ids_by_user.keys())
        if len(created_by_ids) < min_created_by:
            continue

        created_by_map: dict[str, dict[str, str | int | None]] = {}
        for user_id in created_by_ids:
            created_by_map[user_id] = {
                "email": query_user_email(
                    api_url,
                    account_id,
                    user_id,
                    logger,
                    user_email_cache,
                ),
                "signals": len(signal_ids_by_user[user_id]),
            }

        rows.append(
            {
                "workspace.name": workspace_name,
                "workspace.id": account_id,
                "connection.platform.id": platform_id,
                "connection.platform.name": bucket["connection.platform.name"],
                "createdBy.count": len(created_by_ids),
                "createdBy": created_by_map,
            }
        )

    logger.info(
        "Workspace %s: %s signals, %s platforms, %s with multiple createdBy",
        workspace_name,
        len(signals),
        len(grouped),
        len(rows),
    )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find connection.platform sources with multiple distinct "
            "signal.createdBy values across workspaces."
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
        "--min-created-by",
        type=int,
        default=2,
        help="Minimum unique createdBy count to include (default: 2)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="JSON output path (default: data/multiple_createdBy_per_source_<timestamp>.json)",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        default=None,
        help="CSV output path (default: same stem as JSON with .csv)",
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
            workspace
            for workspace in workspaces
            if workspace["name"] in customer_filter
        ]
        missing = customer_filter - {workspace["name"] for workspace in workspaces}
        if missing:
            logger.warning("Workspace(s) not found: %s", ", ".join(sorted(missing)))

    if not workspaces:
        logger.info("No workspaces to process.")
        return

    all_rows: list[dict] = []
    user_email_cache: dict[str, str | None] = {}

    for workspace in workspaces:
        logger.info("=== Workspace: %s (%s) ===", workspace["name"], workspace["id"])
        rows = process_workspace(
            api_url,
            workspace,
            logger,
            user_email_cache,
            min_created_by=args.min_created_by,
        )
        all_rows.extend(rows)

    default_json, default_csv = default_output_paths()
    json_path = args.output or default_json
    csv_path = args.csv_output or (
        json_path.with_suffix(".csv") if args.output else default_csv
    )

    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(all_rows, fh, indent=2, default=str)

    csv_rows = [
        {
            **{k: v for k, v in row.items() if k != "createdBy"},
            "createdBy": json.dumps(row["createdBy"], sort_keys=True),
        }
        for row in all_rows
    ]
    pd.DataFrame(csv_rows).to_csv(csv_path, index=False)

    logger.info("Wrote %s rows to %s", len(all_rows), json_path.resolve())
    logger.info("Wrote %s rows to %s", len(all_rows), csv_path.resolve())
    logger.info(
        "Summary: workspaces=%s | platforms_with_multiple_createdBy=%s | "
        "unique_users_resolved=%s",
        len(workspaces),
        len(all_rows),
        sum(1 for email in user_email_cache.values() if email),
    )


if __name__ == "__main__":
    main()
