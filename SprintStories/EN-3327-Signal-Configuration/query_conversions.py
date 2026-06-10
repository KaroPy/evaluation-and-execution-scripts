"""
Query conversions for all workspaces over the last N days.

Mirrors the targeting API conversions query used in
DataChecks/conversions/conversions.ipynb and
innkeepr-analytics/src/utils/api_conversions_query.py.

Writes a log file with per-workspace conversion counts and full dataframes.
Queries the API day by day and concatenates the results.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from check_signal_configuration import api_post
from src.paths import CONVERSIONS_LOGS_DIR as LOGS_DIR, SCRIPT_DIR

DEFAULT_DAYS = 14
DEFAULT_GOAL = "checkout_completed"

WORKSPACE_GOALS: dict[str, list[str]] = {
    "More": ["checkout_completed"],
    "ESN": ["checkout_completed"],
    "Whacky Food": ["checkout_completed"],
    "Pendix": ["erwaegung"],
    "LILLYDOO": ["checkout_completed"],
    "Plantura": ["checkout_completed"],
    "Rosental": ["checkout_completed"],
    "Ective": ["checkout_completed"],
    "Clockin": ["tagmanagerregistercompleted"],
    "Störtebekker": ["checkout_completed"],
    "MissPompadour GmbH": ["order completed"],
    "ahead-nutrition.com": ["checkout_completed"],
    "to teach": ["upgraded_plan"],
    "Tchibo": ["checkout completed"],
}


def get_workspace_goals(workspace_name: str) -> list[str]:
    return WORKSPACE_GOALS.get(workspace_name, [DEFAULT_GOAL])


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"conversions_{stamp}.log"


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


def timestamp_milliseconds(date: str | datetime) -> int:
    timestamp = pd.to_datetime(date).replace(tzinfo=timezone.utc).timestamp() * 1000
    return int(timestamp)


def iter_daily_ranges(start_date: datetime, end_date: datetime) -> list[tuple[datetime, datetime]]:
    day_start = start_date.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    final_end = end_date.astimezone(timezone.utc)
    ranges: list[tuple[datetime, datetime]] = []
    current = day_start
    while current.date() <= final_end.date():
        day_end = current.replace(hour=23, minute=59, second=59, microsecond=999000)
        if day_end > final_end:
            day_end = final_end
        ranges.append((current, day_end))
        current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return ranges


def query_conversions_for_range(
    api_url: str,
    token: str,
    workspace_id: str,
    start_date: datetime,
    end_date: datetime,
    goals: list[str],
) -> pd.DataFrame:
    content = {
        "created": {
            "$gte": timestamp_milliseconds(start_date),
            "$lte": timestamp_milliseconds(end_date),
        },
        "name": goals,
    }
    page = 1
    results: list[dict] = []
    endpoint = f"{api_url.rstrip('/')}/api/conversions/query"

    while True:
        payload = {
            "content": content,
            "context": {"workspaceId": workspace_id},
            "pagination": {"page": page},
        }
        response = requests.post(
            endpoint,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("messages"):
            message = body["messages"][0]
            if message.get("type") in {"exception", "error"}:
                raise RuntimeError(json.dumps(body))

        results.extend(body.get("data", []))
        next_page = body.get("pagination", {}).get("next")
        if not next_page:
            break
        page = next_page

    if not results:
        return pd.DataFrame()

    conversions = pd.json_normalize(results)
    if "id" in conversions.columns:
        conversions = conversions.rename(columns={"id": "_id"})
    return conversions.drop_duplicates(subset=["sessionId"]).reset_index(drop=True)


def query_conversions_daily(
    api_url: str,
    token: str,
    workspace_id: str,
    start_date: datetime,
    end_date: datetime,
    goals: list[str],
) -> pd.DataFrame:
    conversions = pd.DataFrame()
    for day_start, day_end in iter_daily_ranges(start_date, end_date):
        day_label = day_start.strftime("%Y-%m-%d")
        day_data = query_conversions_for_range(
            api_url, token, workspace_id, day_start, day_end, goals
        )
        logging.info("  %s: %s conversions", day_label, len(day_data))
        if day_data.empty:
            continue
        conversions = pd.concat([conversions, day_data], ignore_index=True)
        conversions = conversions.drop_duplicates().reset_index(drop=True)
    return conversions


def log_workspace_conversions(
    workspace_name: str,
    workspace_id: str,
    goals: list[str],
    conversions: pd.DataFrame,
) -> None:
    logging.info("#### %s ###", workspace_name)
    logging.info("workspace.id: %s", workspace_id)
    logging.info("goals: %s", ", ".join(goals))
    logging.info("conversion count: %s", len(conversions))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query conversions for all workspaces and write a log file."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="Number of days to look back (default: 14)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Path for log output (default: conversions/logs/conversions_<timestamp>.log)",
    )
    parser.add_argument(
        "--workspace",
        action="append",
        dest="workspaces",
        metavar="NAME",
        help="Limit to one or more workspace names",
    )
    return parser.parse_args()


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


def main() -> None:
    args = parse_args()
    load_dotenv(SCRIPT_DIR / ".env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logging.info("Logging to %s", log_path)

    api_url = os.environ["TARGETING_URL"].rstrip("/")
    token = os.environ["API_SERVICE_TOKEN"]
    workspace_filter = normalize_workspace_filter(args.workspaces)

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=args.days)
    logging.info(
        "Querying conversions from %s to %s (%s days)",
        start_date.strftime("%Y-%m-%d %H:%M:%S UTC"),
        end_date.strftime("%Y-%m-%d %H:%M:%S UTC"),
        args.days,
    )

    workspaces = api_post(f"{api_url}/api/core/workspaces/query", token, {"content": {}})
    total_conversions = 0

    for workspace in workspaces:
        workspace_name = workspace["name"]
        if workspace_filter and workspace_name not in workspace_filter:
            continue

        workspace_id = workspace["id"]
        goals = get_workspace_goals(workspace_name)
        logging.info("Querying workspace: %s (goals: %s)", workspace_name, ", ".join(goals))
        try:
            conversions = query_conversions_daily(
                api_url, token, workspace_id, start_date, end_date, goals
            )
            log_workspace_conversions(workspace_name, workspace_id, goals, conversions)
        except Exception as exc:
            logging.info("#### %s ###", workspace_name)
            logging.info("workspace.id: %s", workspace_id)
            logging.exception("Failed to query conversions: %s", exc)
        logging.info("######### FINISHED #########")

    logging.info("Done. Log saved to %s", log_path)


if __name__ == "__main__":
    main()
