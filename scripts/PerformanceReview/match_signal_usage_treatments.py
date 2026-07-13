"""
Match active signals to treatments from signals/usage/query.

For each workspace, loads all active signals, queries signals/usage/query for the
requested date range, and assigns usage rows to signals when usage.targetings
references the signal's externalId or name.

Usage (from repo root):
    python scripts/PerformanceReview/match_signal_usage_treatments.py \\
        --workspace Rosental --from-date 20260101 --to-date 20260131

    python scripts/PerformanceReview/match_signal_usage_treatments.py \\
        --workspace Kfzteile24 --workspace Tchibo --from-date 20251201 --to-date 20251231
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
    send_to_innkeepr_api_paginated,
    validate_response,
)
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402
from src.utils.innkeepr_signal_detector import (  # noqa: E402
    aggregate_signal_matches_by_treatment,
    build_usage_key_to_treatment,
    get_signal_treatment_ids,
    match_signals_to_usage,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_OUTPUT = DATA_DIR / "signal_usage_treatment_matches.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"match_signal_usage_treatments_{stamp}.log"


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


def normalize_date(value: str) -> str:
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return text
    parsed = pd.to_datetime(text, errors="raise")
    return parsed.strftime("%Y%m%d")


def query_all_signals(
    api_url: str,
    account_id: str,
    logger: logging.Logger,
) -> list[dict]:
    endpoint = f"{api_url}/api/signals/query"
    logger.info("Querying %s", endpoint)
    next_page = 1
    results: list[dict] = []
    while next_page is not None:
        payload = json.dumps(
            {
                "content": {"status": "active"},
                "pagination": {"page": next_page},
                "context": {"accountId": account_id},
            }
        )
        json_body = make_http_post_call(endpoint, payload, logger)
        validate_response(json_body, logger)
        results.extend(json_body["data"])
        next_page = (json_body.get("pagination") or {}).get("next")
    return results


def query_signal_usage(
    api_url: str,
    account_id: str,
    from_date: str,
    to_date: str,
    logger: logging.Logger,
) -> list[dict]:
    endpoint = f"{api_url}/api/signals/usage/query"
    content = {"fromDate": from_date, "toDate": to_date}
    return call_api_with_accountId(endpoint, account_id, content, logger)


def fetch_treatments_by_id(
    api_url: str,
    account_id: str,
    treatment_ids: list[str],
    logger: logging.Logger,
) -> dict[str, dict]:
    if not treatment_ids:
        return {}

    treatments = send_to_innkeepr_api_paginated(
        f"{api_url}/api/treatments/query",
        account_id,
        {"id": treatment_ids},
        logger,
    )
    return {str(item["id"]): item for item in treatments if item.get("id")}


def fetch_treatments_by_source(
    api_url: str,
    account_id: str,
    source_ids: list[str],
    logger: logging.Logger,
) -> dict[str, dict]:
    treatments_by_id: dict[str, dict] = {}
    for source_id in source_ids:
        treatments = send_to_innkeepr_api_paginated(
            f"{api_url}/api/treatments/query",
            account_id,
            {"source": source_id},
            logger,
        )
        for item in treatments:
            treatment_id = item.get("id")
            if treatment_id:
                treatments_by_id[str(treatment_id)] = item
    return treatments_by_id


def enrich_treatments_for_usage(
    api_url: str,
    account_id: str,
    usage_rows: list[dict],
    treatments_by_id: dict[str, dict],
    logger: logging.Logger,
) -> dict[str, dict]:
    if not usage_rows:
        return treatments_by_id

    usage_df = pd.json_normalize(usage_rows)
    usage_keys = {
        str(value)
        for value in usage_df.get("treatment", pd.Series(dtype=object)).dropna().unique()
    }
    if not usage_keys:
        return treatments_by_id

    lookup = build_usage_key_to_treatment(treatments_by_id)
    unresolved_keys = {key for key in usage_keys if key not in lookup}
    if not unresolved_keys:
        return treatments_by_id

    source_ids = sorted(
        {
            str(value)
            for value in usage_df.get("connectionId", pd.Series(dtype=object)).dropna().unique()
        }
    )
    if not source_ids:
        logger.warning(
            "Could not resolve %s usage treatments (no connectionId)",
            len(unresolved_keys),
        )
        return treatments_by_id

    logger.info(
        "Fetching treatments for %s source(s) to resolve %s usage treatment keys",
        len(source_ids),
        len(unresolved_keys),
    )
    source_treatments = fetch_treatments_by_source(api_url, account_id, source_ids, logger)
    treatments_by_id = {**source_treatments, **treatments_by_id}

    lookup = build_usage_key_to_treatment(treatments_by_id)
    still_unresolved = [key for key in unresolved_keys if key not in lookup]
    if still_unresolved:
        logger.warning(
            "Could not resolve %s usage treatment keys to treatments",
            len(still_unresolved),
        )
    return treatments_by_id


def build_workspace_report(
    api_url: str,
    workspace: dict,
    from_date: str,
    to_date: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    account_id = workspace["id"]
    workspace_name = workspace["name"]

    signals = query_all_signals(api_url, account_id, logger)
    logger.info("Workspace %s: %s active signals", workspace_name, len(signals))
    if not signals:
        return pd.DataFrame()

    usage_rows = query_signal_usage(api_url, account_id, from_date, to_date, logger)
    logger.info("Workspace %s: %s usage rows", workspace_name, len(usage_rows))

    treatment_ids: set[str] = set()
    for signal in signals:
        treatment_ids.update(get_signal_treatment_ids(signal))

    treatments_by_id = fetch_treatments_by_id(
        api_url,
        account_id,
        sorted(treatment_ids),
        logger,
    )
    logger.info(
        "Workspace %s: resolved %s/%s configured treatments",
        workspace_name,
        len(treatments_by_id),
        len(treatment_ids),
    )

    treatments_by_id = enrich_treatments_for_usage(
        api_url,
        account_id,
        usage_rows,
        treatments_by_id,
        logger,
    )

    matches = match_signals_to_usage(signals, usage_rows)
    return aggregate_signal_matches_by_treatment(matches, treatments_by_id, workspace_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match active signals to treatments from signals/usage/query."
    )
    parser.add_argument(
        "--workspace",
        action="append",
        dest="workspaces",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --workspace Rosental",
    )
    parser.add_argument(
        "--from-date",
        required=True,
        metavar="DATE",
        help="Start date (YYYYMMDD or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        metavar="DATE",
        help="End date (YYYYMMDD or YYYY-MM-DD)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV output path (default: {DEFAULT_OUTPUT.name})",
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

    from_date = normalize_date(args.from_date)
    to_date = normalize_date(args.to_date)
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

    report_frames: list[pd.DataFrame] = []
    for workspace in workspaces:
        logger.info("=== Workspace: %s (%s) ===", workspace["name"], workspace["id"])
        report_frames.append(build_workspace_report(api_url, workspace, from_date, to_date, logger))

    report = pd.concat(report_frames, ignore_index=True) if report_frames else pd.DataFrame()

    if not report.empty:
        report["signal.ids"] = report["signal.ids"].apply(json.dumps)
        report["signal.names"] = report["signal.names"].apply(json.dumps)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.output, index=False)
    logger.info("Saved %s matched rows to %s", len(report), args.output)

    if report.empty:
        print("No signal/usage treatment matches found.")
        return

    pd.set_option("display.max_rows", 200)
    pd.set_option("display.width", 240)
    print(report.to_string(index=False))


if __name__ == "__main__":
    main()
