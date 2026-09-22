"""
Find workspaces whose features_view_30_outlook table was not updated recently.

A workspace is added to possible_outdated_workspaces only when both are true:
  1. trackingOptions.eventTrackingStarted is not null
  2. innkeepr_databricks.<workspace.id>.features_view_30_outlook is missing or
     was not updated in the last 24 hours

Usage (from repo root):
    python scripts/workspace-check/check_outdated_workspaces.py
    python scripts/workspace-check/check_outdated_workspaces.py --hours 24
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import certifi
import pandas as pd
import requests
from databricks import sql
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.constants import return_api_url, return_service_token  # noqa: E402

DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"

DATABRICKS_CATALOG = "innkeepr_databricks"
TABLE_NAME = "features_view_30_outlook"
DEFAULT_FRESHNESS_HOURS = 24
REASON_MISSING = "table_missing"
REASON_STALE = "not_updated_within_threshold"


def connect_databricks():
    host = os.environ["DATABRICKS_HOST"].strip()
    if host.startswith(("http://", "https://")):
        server_hostname = urlparse(host).netloc
    else:
        server_hostname = host.rstrip("/")

    http_path = os.environ.get("DATABRICKS_HTTP_PATH") or os.environ.get(
        "DATABRICKS_WAREHOUSE_ID"
    )
    if not http_path:
        raise RuntimeError("DATABRICKS_HTTP_PATH or DATABRICKS_WAREHOUSE_ID must be set")

    access_token = os.environ.get("DATABRICKS_TOKEN") or os.environ.get("BEARER_TOKEN")
    if not access_token:
        raise RuntimeError("DATABRICKS_TOKEN or BEARER_TOKEN must be set")

    ca_bundle = (
        os.environ.get("SSL_CERT_FILE") or os.environ.get("REQUESTS_CA_BUNDLE") or certifi.where()
    )
    os.environ.setdefault("SSL_CERT_FILE", ca_bundle)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", ca_bundle)

    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        query_tags={"account": "DatabricksSQL"},
        _tls_trusted_ca_file=ca_bundle,
        _connect_timeout=60,
        _socket_timeout=300,
    )


def default_output_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"possible_outdated_workspaces_{stamp}.json"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"check_outdated_workspaces_{stamp}.log"


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.getLogger("databricks").setLevel(logging.WARNING)

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List workspaces with eventTrackingStarted set whose "
            "features_view_30_outlook table is missing or was not updated "
            "within the freshness window."
        )
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=DEFAULT_FRESHNESS_HOURS,
        help=f"Treat tables older than this as outdated (default: {DEFAULT_FRESHNESS_HOURS})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="JSON output path (default: scripts/workspace-check/data/possible_outdated_workspaces_<timestamp>.json)",
    )
    return parser.parse_args()


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def features_table_name(workspace_id: str) -> str:
    return ".".join(
        quote_ident(part) for part in (DATABRICKS_CATALOG, workspace_id, TABLE_NAME)
    )


def to_utc(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def column_value(frame: pd.DataFrame, *names: str) -> object:
    if frame.empty:
        return None
    lowered = {str(column).lower(): column for column in frame.columns}
    for name in names:
        column = lowered.get(name.lower())
        if column is not None:
            return frame.iloc[0][column]
    return None


def fetch_existing_tables(cursor) -> dict[str, dict]:
    cursor.execute(
        f"""
        SELECT table_schema, table_name, table_type, last_altered
        FROM {quote_ident(DATABRICKS_CATALOG)}.information_schema.tables
        WHERE lower(table_name) = '{TABLE_NAME}'
        """
    )
    frame = cursor.fetchall_arrow().to_pandas()
    tables: dict[str, dict] = {}
    if frame.empty:
        return tables
    for _, row in frame.iterrows():
        schema = str(row["table_schema"]).lower()
        tables[schema] = {
            "table_type": None if pd.isna(row["table_type"]) else str(row["table_type"]),
            "last_altered": to_utc(row["last_altered"]),
        }
    return tables


def fetch_last_modified(cursor, workspace_id: str) -> datetime | None:
    cursor.execute(f"DESCRIBE DETAIL {features_table_name(workspace_id)}")
    detail = cursor.fetchall_arrow().to_pandas()
    return to_utc(column_value(detail, "lastModified", "last_modified"))


def load_workspaces_with_tracking() -> list[dict]:
    url = f"{return_api_url()}api/core/workspaces/query"
    payload = json.dumps({"content": {}, "context": {"serviceToken": return_service_token()}})
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {return_service_token()}",
    }
    response = requests.post(url, headers=headers, data=payload, timeout=60)
    response.raise_for_status()

    workspaces = []
    for entry in response.json()["data"]:
        tracking_options = entry.get("trackingOptions") or {}
        event_tracking_started = tracking_options.get("eventTrackingStarted")
        if event_tracking_started is None:
            continue
        workspaces.append(
            {
                "id": entry["id"],
                "name": entry["name"],
                "eventTrackingStarted": event_tracking_started,
            }
        )
    return workspaces


def workspace_record(
    workspace: dict,
    *,
    table_exists: bool,
    last_updated: datetime | None,
    reason: str,
    table_type: str | None = None,
) -> dict:
    return {
        "id": workspace["id"],
        "name": workspace["name"],
        "eventTrackingStarted": workspace["eventTrackingStarted"],
        "table": f"{DATABRICKS_CATALOG}.{workspace['id']}.{TABLE_NAME}",
        "table_exists": table_exists,
        "table_type": table_type,
        "last_updated": isoformat(last_updated),
        "reason": reason,
    }


def check_workspaces(workspaces: list[dict], cutoff: datetime) -> list[dict]:
    possible_outdated_workspaces: list[dict] = []

    with connect_databricks() as connection:
        with connection.cursor() as cursor:
            existing_tables = fetch_existing_tables(cursor)
            logging.info(
                "Found %s %s tables in %s",
                len(existing_tables),
                TABLE_NAME,
                DATABRICKS_CATALOG,
            )

            for index, workspace in enumerate(workspaces, start=1):
                workspace_id = workspace["id"]
                table_info = existing_tables.get(workspace_id.lower())
                logging.info(
                    "[%s/%s] %s (%s)",
                    index,
                    len(workspaces),
                    workspace["name"],
                    workspace_id,
                )

                if table_info is None:
                    possible_outdated_workspaces.append(
                        workspace_record(
                            workspace,
                            table_exists=False,
                            last_updated=None,
                            reason=REASON_MISSING,
                        )
                    )
                    continue

                last_updated = table_info["last_altered"]
                try:
                    last_modified = fetch_last_modified(cursor, workspace_id)
                except Exception as exc:
                    logging.warning(
                        "DESCRIBE DETAIL failed for %s; using last_altered. %s",
                        workspace_id,
                        exc,
                    )
                else:
                    if last_modified is not None:
                        last_updated = last_modified

                if last_updated is None or last_updated < cutoff:
                    possible_outdated_workspaces.append(
                        workspace_record(
                            workspace,
                            table_exists=True,
                            last_updated=last_updated,
                            reason=REASON_STALE,
                            table_type=table_info["table_type"],
                        )
                    )

    return possible_outdated_workspaces


def main() -> None:
    load_dotenv()
    args = parse_args()
    if args.hours <= 0:
        raise SystemExit("--hours must be greater than 0")

    log_path = default_log_path()
    setup_logging(log_path)
    output_path = args.output or default_output_path()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    workspaces = load_workspaces_with_tracking()
    logging.info(
        "Checking %s workspaces with eventTrackingStarted set (updated since %s)",
        len(workspaces),
        cutoff.isoformat(),
    )

    possible_outdated_workspaces = check_workspaces(workspaces, cutoff)

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "catalog": DATABRICKS_CATALOG,
        "table": TABLE_NAME,
        "freshness_hours": args.hours,
        "cutoff": cutoff.isoformat(),
        "workspaces_checked": len(workspaces),
        "possible_outdated_workspaces": possible_outdated_workspaces,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    logging.info(
        "possible_outdated_workspaces (%s of %s)",
        len(possible_outdated_workspaces),
        len(workspaces),
    )
    for workspace in possible_outdated_workspaces:
        logging.info(
            "  %s (%s) %s last_updated=%s",
            workspace["name"],
            workspace["id"],
            workspace["reason"],
            workspace["last_updated"],
        )
    logging.info("Wrote %s", output_path)
    logging.info("Log: %s", log_path)


if __name__ == "__main__":
    main()
