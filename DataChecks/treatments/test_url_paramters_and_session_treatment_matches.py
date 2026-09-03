"""
Check whether connection URL tracking/campaign parameters appear in recent
sessions and resolve matching treatment ids.

For each workspace (or --customer):
  1. Query sessions from the last 24 hours (sessions/query)
  2. Query connections (connections/query)
  3. Query treatments for each connection.id (treatments/query)
  4. Check whether connection.options.urlTrackingParam and/or
     urlCampaignParam (googleAdwords) appear as campaign.* fields on sessions
  5. Resolve matching treatment ids:
       - googleAdwords:
           * campaign: session campaign.{urlCampaignParam} ↔ treatments.relates_to.campaign.id
           * tracking: ads/query {"externalId": session tracking values}
             then ads.relates_to.treatment intersected with connection treatments
         (separate counts for tracking and campaign)
       - other platforms: ads/query {"externalId": session tracking values}
         then ads.relates_to.treatment intersected with treatments for that
         connection
  6. Count findings and write CSV/JSON

Usage (from repo root):
    python DataChecks/treatments/test_url_paramters_and_session_treatment_matches.py
    python DataChecks/treatments/test_url_paramters_and_session_treatment_matches.py \\
        --customer More
    python DataChecks/treatments/test_url_paramters_and_session_treatment_matches.py \\
        --customer Tchibo \\
        --start-date 2026-08-10T15:00:00 --end-date 2026-08-10T20:00:00
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))

from general_functions.call_api_with_account_id import (  # ruff: ignore[module-import-not-at-top-of-file]
    call_api_with_accountId,
    send_to_innkeepr_api_paginated,
)
from general_functions.constants import (
    return_api_url,
)
from general_functions.return_workspace_ids import (
    return_workspace_ids,
)

DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
GOOGLE_PLATFORM = "googleAdwords"
PMAX_TREATMENT_TYPE = "PMAX"
ACTIVE_TREATMENT_STATUS = "active"
ADS_QUERY_BATCH_SIZE = 2000


def default_output_paths() -> tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"url_param_session_treatment_matches_{stamp}"
    return DATA_DIR / f"{stem}.csv", DATA_DIR / f"{stem}.json"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"url_param_session_treatment_matches_{stamp}.log"


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


def api_path(api_url: str, endpoint: str) -> str:
    return f"{api_url.rstrip('/')}/api/{endpoint.lstrip('/')}"


def sessions_since_iso(hours: float = 24.0) -> str:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    return since.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def normalize_session_datetime(value: str) -> str:
    """
    Accept ISO-like datetimes, optionally without timezone / seconds / Z,
    and return a UTC ISO string usable by sessions/query.
    """
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SystemExit(
            f"Invalid datetime '{value}'. Use e.g. 2026-08-10T15:00:00 "
            "or 2026-08-10T15:00:00Z"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def resolve_session_time_range(args: argparse.Namespace) -> tuple[str, str | None]:
    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("Provide both --start-date and --end-date together.")
        start = normalize_session_datetime(args.start_date)
        end = normalize_session_datetime(args.end_date)
        if start >= end:
            raise SystemExit(
                f"--start-date ({start}) must be before --end-date ({end})."
            )
        return start, end
    return sessions_since_iso(args.hours), None


def query_sessions(
    api_url: str,
    account_id: str,
    since_iso: str,
    logger: logging.Logger,
    until_iso: str | None = None,
) -> pd.DataFrame:
    created_filter: dict[str, str] = {"$gte": since_iso}
    if until_iso:
        created_filter["$lt"] = until_iso
    content = {"created": created_filter}
    rows = send_to_innkeepr_api_paginated(
        api_path(api_url, "sessions/query"),
        account_id,
        content,
        logger,
    )
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def query_connections(
    api_url: str,
    account_id: str,
    logger: logging.Logger,
) -> list[dict]:
    return (
        call_api_with_accountId(
            api_path(api_url, "connections/query"),
            account_id,
            {},
            logger,
        )
        or []
    )


def query_treatments_for_connection(
    api_url: str,
    account_id: str,
    connection_id: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    rows = send_to_innkeepr_api_paginated(
        api_path(api_url, "treatments/query"),
        account_id,
        {"connection": connection_id},
        logger,
    )
    if not rows:
        return pd.DataFrame()
    return pd.json_normalize(rows)


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def query_ads_by_external_ids(
    api_url: str,
    account_id: str,
    external_ids: list[str],
    logger: logging.Logger,
) -> pd.DataFrame:
    if not external_ids:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for batch in chunked(external_ids, ADS_QUERY_BATCH_SIZE):
        rows = send_to_innkeepr_api_paginated(
            api_path(api_url, "ads/query"),
            account_id,
            {"externalId": batch},
            logger,
        )
        if rows:
            frames.append(pd.json_normalize(rows))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def connection_param(connection: dict, key: str) -> str | None:
    options = connection.get("options") or {}
    value = options.get(key)
    if value is None or value == "":
        return None
    return str(value)


def campaign_column(param: str | None) -> str | None:
    if not param:
        return None
    return f"campaign.{param}"


def unique_non_null_values(sessions: pd.DataFrame, column: str | None) -> list[str]:
    if not column or column not in sessions.columns or sessions.empty:
        return []
    series = sessions[column].dropna().astype(str)
    series = series[series.str.len() > 0]
    return sorted(series.unique().tolist())


def sessions_with_value_count(sessions: pd.DataFrame, column: str | None) -> int:
    if not column or column not in sessions.columns or sessions.empty:
        return 0
    return int(sessions[column].notna().sum())


def treatment_properties_type_value_counts(
    treatments: pd.DataFrame,
    matched_treatment_ids: list[str],
) -> dict[str, int]:
    if (
        treatments.empty
        or not matched_treatment_ids
        or "id" not in treatments.columns
        or "properties.type" not in treatments.columns
    ):
        return {}

    matched = treatments[
        treatments["id"].astype(str).isin({str(value) for value in matched_treatment_ids})
    ]
    if matched.empty:
        return {}

    counts = matched["properties.type"].fillna("null").astype(str).value_counts()
    return {str(key): int(value) for key, value in counts.items()}


def count_active_pmax_treatments(treatments: pd.DataFrame) -> int:
    if treatments.empty or "properties.type" not in treatments.columns:
        return 0

    type_mask = treatments["properties.type"].astype(str) == PMAX_TREATMENT_TYPE
    if "properties.status" in treatments.columns:
        status_mask = treatments["properties.status"].astype(str) == ACTIVE_TREATMENT_STATUS
        return int((type_mask & status_mask).sum())
    return int(type_mask.sum())


def match_google_adwords_campaign_param(
    sessions: pd.DataFrame,
    treatments: pd.DataFrame,
    url_campaign_param: str | None,
) -> dict:
    campaign_col = campaign_column(url_campaign_param)
    session_campaign_ids = unique_non_null_values(sessions, campaign_col)

    treatment_campaign_ids: set[str] = set()
    treatment_ids_by_campaign: dict[str, list[str]] = {}
    if not treatments.empty and "relates_to.campaign.id" in treatments.columns:
        for _, row in treatments.iterrows():
            campaign_id = row.get("relates_to.campaign.id")
            treatment_id = row.get("id")
            if campaign_id is None or treatment_id is None:
                continue
            campaign_key = str(campaign_id)
            treatment_campaign_ids.add(campaign_key)
            treatment_ids_by_campaign.setdefault(campaign_key, []).append(str(treatment_id))

    matched_campaign_ids = sorted(set(session_campaign_ids) & treatment_campaign_ids)
    unmatched_campaign_ids = sorted(set(session_campaign_ids) - treatment_campaign_ids)
    matched_treatment_ids = sorted(
        {
            treatment_id
            for campaign_id in matched_campaign_ids
            for treatment_id in treatment_ids_by_campaign.get(campaign_id, [])
        }
    )
    type_counts = treatment_properties_type_value_counts(treatments, matched_treatment_ids)

    return {
        "campaign.match.method": "urlCampaignParam->treatments.relates_to.campaign.id",
        "campaign.session.param_values": len(session_campaign_ids),
        "campaign.matched.param_values": len(matched_campaign_ids),
        "campaign.unmatched.param_values": len(unmatched_campaign_ids),
        "campaign.matched.treatment_ids": matched_treatment_ids,
        "campaign.matched.treatment_count": len(matched_treatment_ids),
        "campaign.matched.param_ids": matched_campaign_ids,
        "campaign.unmatched.param_ids": unmatched_campaign_ids,
        "campaign.treatment.properties.type.value_counts": type_counts,
    }


def match_ads_to_treatments(
    api_url: str,
    account_id: str,
    sessions: pd.DataFrame,
    treatments: pd.DataFrame,
    url_tracking_param: str | None,
    logger: logging.Logger,
    *,
    field_prefix: str = "",
    include_type_counts: bool = False,
) -> dict:
    tracking_col = campaign_column(url_tracking_param)
    external_ids = unique_non_null_values(sessions, tracking_col)
    treatment_ids = set()
    if not treatments.empty and "id" in treatments.columns:
        treatment_ids = {str(value) for value in treatments["id"].dropna().tolist()}

    ads = query_ads_by_external_ids(api_url, account_id, external_ids, logger)
    ad_treatment_ids: list[str] = []
    if not ads.empty and "relates_to.treatment" in ads.columns:
        ad_treatment_ids = [str(value) for value in ads["relates_to.treatment"].dropna().tolist()]

    matched_treatment_ids = sorted(set(ad_treatment_ids) & treatment_ids)
    unmatched_ad_treatment_ids = sorted(set(ad_treatment_ids) - treatment_ids)

    matched_external_ids: list[str] = []
    unmatched_external_ids = list(external_ids)
    if not ads.empty and "externalId" in ads.columns:
        if "relates_to.treatment" in ads.columns:
            ads_with_match = ads[
                ads["relates_to.treatment"].astype(str).isin(matched_treatment_ids)
            ]
            matched_external_ids = sorted(
                {str(value) for value in ads_with_match["externalId"].dropna().tolist()}
            )
        known_external = {str(value) for value in ads["externalId"].dropna().tolist()}
        unmatched_external_ids = sorted(set(external_ids) - known_external)

    prefix = f"{field_prefix}." if field_prefix else ""
    result = {
        f"{prefix}match.method": "urlTrackingParam->ads.externalId->ads.relates_to.treatment",
        f"{prefix}session.param_values": len(external_ids),
        f"{prefix}ads.returned": len(ads),
        f"{prefix}matched.param_values": len(matched_external_ids),
        f"{prefix}unmatched.param_values": len(unmatched_external_ids),
        f"{prefix}matched.treatment_ids": matched_treatment_ids,
        f"{prefix}matched.treatment_count": len(matched_treatment_ids),
        f"{prefix}ads.treatment_ids_not_in_connection": unmatched_ad_treatment_ids,
        f"{prefix}matched.param_ids": matched_external_ids,
        f"{prefix}unmatched.param_ids": unmatched_external_ids,
    }
    if include_type_counts:
        result[f"{prefix}treatment.properties.type.value_counts"] = (
            treatment_properties_type_value_counts(treatments, matched_treatment_ids)
        )
    return result


def process_connection(
    api_url: str,
    account_id: str,
    workspace_name: str,
    connection: dict,
    sessions: pd.DataFrame,
    logger: logging.Logger,
) -> dict:
    connection_id = connection.get("id")
    connection_name = connection.get("name")
    url_tracking_param = connection_param(connection, "urlTrackingParam")
    url_campaign_param = connection_param(connection, "urlCampaignParam")

    tracking_col = campaign_column(url_tracking_param)
    campaign_col = campaign_column(url_campaign_param)

    tracking_in_sessions = bool(tracking_col and tracking_col in sessions.columns)
    campaign_in_sessions = bool(campaign_col and campaign_col in sessions.columns)

    treatments = query_treatments_for_connection(api_url, account_id, str(connection_id), logger)

    connection_status = connection.get("status")
    row: dict = {
        "workspace.name": workspace_name,
        "workspace.id": account_id,
        "connection.id": connection_id,
        "connection.name": connection_name,
        "connection.status": connection_status,
        "source.status": connection_status,
        "source.statusText": connection.get("statusText"),
        "urlTrackingParam": url_tracking_param,
        "urlCampaignParam": url_campaign_param,
        "sessions.total": len(sessions),
        "urlTrackingParam.column": tracking_col,
        "urlTrackingParam.in_sessions": tracking_in_sessions,
        "urlTrackingParam.session_rows": sessions_with_value_count(sessions, tracking_col),
        "urlCampaignParam.column": campaign_col,
        "urlCampaignParam.in_sessions": campaign_in_sessions,
        "urlCampaignParam.session_rows": sessions_with_value_count(sessions, campaign_col),
        "treatments.connection_count": len(treatments),
    }

    is_google = connection_name == GOOGLE_PLATFORM
    if is_google:
        row["treatments.active_pmax_count"] = count_active_pmax_treatments(treatments)
        campaign_match = match_google_adwords_campaign_param(
            sessions, treatments, url_campaign_param
        )
        tracking_match = match_ads_to_treatments(
            api_url,
            account_id,
            sessions,
            treatments,
            url_tracking_param,
            logger,
            field_prefix="tracking",
            include_type_counts=True,
        )
        row.update(campaign_match)
        row.update(tracking_match)
        row["campaign.findings.count"] = int(row.get("campaign.matched.treatment_count") or 0)
        row["tracking.findings.count"] = int(row.get("tracking.matched.treatment_count") or 0)
        row["findings.count"] = row["campaign.findings.count"] + row["tracking.findings.count"]
        logger.info(
            "  %s (status=%s) | tracking=%s rows=%s matched_treatments=%s types=%s | "
            "campaign=%s rows=%s matched_treatments=%s types=%s | "
            "treatments=%s active_pmax=%s",
            connection_name,
            connection_status,
            url_tracking_param,
            row["urlTrackingParam.session_rows"],
            row["tracking.findings.count"],
            row.get("tracking.treatment.properties.type.value_counts"),
            url_campaign_param,
            row["urlCampaignParam.session_rows"],
            row["campaign.findings.count"],
            row.get("campaign.treatment.properties.type.value_counts"),
            row["treatments.connection_count"],
            row["treatments.active_pmax_count"],
        )
    else:
        match = match_ads_to_treatments(
            api_url,
            account_id,
            sessions,
            treatments,
            url_tracking_param,
            logger,
        )
        row.update(match)
        row["findings.count"] = int(row.get("matched.treatment_count") or 0)
        logger.info(
            "  %s (status=%s) | tracking=%s in_sessions=%s rows=%s | "
            "campaign=%s in_sessions=%s rows=%s | treatments=%s | matched_treatments=%s",
            connection_name,
            connection_status,
            url_tracking_param,
            tracking_in_sessions,
            row["urlTrackingParam.session_rows"],
            url_campaign_param,
            campaign_in_sessions,
            row["urlCampaignParam.session_rows"],
            row["treatments.connection_count"],
            row["findings.count"],
        )

    return row


def process_workspace(
    api_url: str,
    workspace: dict,
    since_iso: str,
    logger: logging.Logger,
    until_iso: str | None = None,
) -> list[dict]:
    account_id = workspace["id"]
    workspace_name = workspace["name"]
    logger.info("=== Workspace: %s (%s) ===", workspace_name, account_id)

    sessions = query_sessions(
        api_url, account_id, since_iso, logger, until_iso=until_iso
    )
    if until_iso:
        logger.info(
            "  Sessions %s .. %s: %s", since_iso, until_iso, len(sessions)
        )
    else:
        logger.info("  Sessions since %s: %s", since_iso, len(sessions))

    connections = query_connections(api_url, account_id, logger)
    advertising = [
        connection
        for connection in connections
        if connection.get("category") == "advertising"
        or connection.get("options", {}).get("urlTrackingAvailable")
        or connection.get("options", {}).get("urlTrackingParam")
        or connection.get("options", {}).get("urlCampaignParam")
    ]
    if not advertising:
        advertising = connections

    logger.info(
        "  Connections: %s (checking %s)",
        len(connections),
        len(advertising),
    )

    rows: list[dict] = []
    for connection in advertising:
        rows.append(
            process_connection(
                api_url,
                account_id,
                workspace_name,
                connection,
                sessions,
                logger,
            )
        )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Match session URL tracking/campaign parameters to treatments "
            "via campaign id (googleAdwords) or ads.externalId (other platforms)."
        )
    )
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --customer More",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Look back window for sessions when no --start-date/--end-date (default: 24)",
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        metavar="DATETIME",
        default=None,
        help="Session range start, e.g. 2026-08-10T15:00:00 (requires --end-date)",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        metavar="DATETIME",
        default=None,
        help="Session range end (exclusive), e.g. 2026-08-10T20:00:00 (requires --start-date)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="CSV output path",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="JSON output path",
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
        workspaces = [ws for ws in workspaces if ws["name"] in customer_filter]
        missing = customer_filter - {ws["name"] for ws in workspaces}
        if missing:
            logger.warning("Workspace(s) not found: %s", ", ".join(sorted(missing)))

    if not workspaces:
        logger.info("No workspaces to process.")
        return

    since_iso, until_iso = resolve_session_time_range(args)
    if until_iso:
        logger.info(
            "Session range: created >= %s and created < %s", since_iso, until_iso
        )
    else:
        logger.info(
            "Session lookback: last %s hours (created >= %s)", args.hours, since_iso
        )

    all_rows: list[dict] = []
    for workspace in workspaces:
        all_rows.extend(
            process_workspace(
                api_url, workspace, since_iso, logger, until_iso=until_iso
            )
        )

    default_csv, default_json = default_output_paths()
    csv_path = args.output or default_csv
    json_path = args.json_output or (csv_path.with_suffix(".json") if args.output else default_json)

    csv_rows = []
    for row in all_rows:
        csv_row = {
            key: (json.dumps(value) if isinstance(value, (list, dict)) else value)
            for key, value in row.items()
        }
        csv_rows.append(csv_row)

    df = pd.DataFrame(csv_rows)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(all_rows, handle, indent=2, default=str)

    total_findings = int(sum(row.get("findings.count") or 0 for row in all_rows))
    google_campaign_findings = int(sum(row.get("campaign.findings.count") or 0 for row in all_rows))
    google_tracking_findings = int(sum(row.get("tracking.findings.count") or 0 for row in all_rows))
    logger.info("Wrote %s connection rows to %s", len(all_rows), csv_path.resolve())
    logger.info("Wrote %s connection rows to %s", len(all_rows), json_path.resolve())
    logger.info(
        "Summary: workspaces=%s | connections=%s | matched_treatments_total=%s "
        "(google campaign=%s, google tracking=%s)",
        len(workspaces),
        len(all_rows),
        total_findings,
        google_campaign_findings,
        google_tracking_findings,
    )


if __name__ == "__main__":
    main()
