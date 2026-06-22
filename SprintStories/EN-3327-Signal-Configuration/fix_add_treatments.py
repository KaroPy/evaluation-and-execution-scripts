"""
Add treatments to audiences that are missing them, using manual campaign rules.

Reads audience_model_treatments/manual_dict_add_treatments.json and
audience_model_treatments_audit.csv (label == add treatments). For each
configured workspace, matches audiences whose name includes a customer key,
queries treatments by source (googleAnalytics audiences use googleAdwords
treatments), filters by campaign name list and regex, then
appends matching treatment ids to the audience config (without overwriting
existing treatments).

Dry-run is the default. Pass --apply to write changes to production.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from check_signal_configuration import api_post, query_all_pages
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR,
    AUDIENCE_MODEL_TREATMENTS_DIR,
    AUDIENCE_MODEL_TREATMENTS_LOGS_DIR as LOGS_DIR,
    SCRIPT_DIR,
)

MANUAL_DICT_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DIR / "manual_dict_add_treatments.json"
)
AUDIT_CSV_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_audit.csv"
)
PLAN_OUTPUT_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "fix_add_treatments_plan.json"
)
LABEL_ADD_TREATMENTS = "add treatments"
GOOGLE_ANALYTICS_SOURCE_NAME = "googleAnalytics"
GOOGLE_ADWORDS_SOURCE_NAME = "googleAdwords"
# Treatments for GA audiences are stored under the googleAdwords connection.
GOOGLE_ADWORDS_TREATMENT_SOURCE_ID = "609ffc4578188e83a2bc2c2c"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"fix_add_treatments_{stamp}.log"


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


def load_manual_dict(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def load_add_treatment_rows(
    audit_path: Path,
    workspace_names: set[str],
) -> pd.DataFrame:
    table = pd.read_csv(audit_path)
    rows = table[
        (table["label"] == LABEL_ADD_TREATMENTS)
        & (table["workspace.name"].isin(workspace_names))
    ].copy()
    return rows.sort_values(["workspace.name", "audience.name"]).reset_index(drop=True)


def match_customer_key(audience_name: str, workspace_config: dict) -> str | None:
    matches = [
        customer_key
        for customer_key in workspace_config
        if customer_key in audience_name
    ]
    if not matches:
        return None
    return max(matches, key=len)


def get_campaign_name(treatment: dict) -> str:
    relates_to = treatment.get("relates_to") or {}
    campaign = relates_to.get("campaign") or {}
    return campaign.get("name") or ""


def treatment_matches_campaign(
    treatment: dict,
    campaign_names: list[str],
    campaign_regex: re.Pattern[str] | None,
) -> bool:
    campaign_name = get_campaign_name(treatment)
    if campaign_name in campaign_names:
        return True
    if campaign_regex and campaign_regex.search(campaign_name):
        return True
    return False


def select_matched_treatments(
    treatments: list[dict],
    customer_config: dict,
) -> list[dict[str, str]]:
    campaign_names = list(customer_config.get("campaign_names") or [])
    regex_value = customer_config.get("campaign_regex")
    campaign_regex = re.compile(regex_value) if regex_value else None
    matched: list[dict[str, str]] = []
    for treatment in treatments:
        treatment_id = treatment.get("id")
        if not treatment_id:
            continue
        if treatment_matches_campaign(treatment, campaign_names, campaign_regex):
            matched.append(
                {
                    "treatment.id": treatment_id,
                    "treatment.campaign.name": get_campaign_name(treatment),
                }
            )
    return matched


def merge_treatment_ids(existing: list[str] | None, new_ids: list[str]) -> list[str]:
    merged = list(existing or [])
    for treatment_id in new_ids:
        if treatment_id not in merged:
            merged.append(treatment_id)
    return merged


def api_post_data(api_url: str, token: str, endpoint: str, payload: dict) -> dict | list:
    response = requests.post(
        f"{api_url}/{endpoint.lstrip('/')}",
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
    return body.get("data", {})


def fetch_audience(api_url: str, token: str, workspace_id: str, audience_id: str) -> dict:
    audiences = api_post(
        f"{api_url}/api/audiences/query",
        token,
        {"content": {"id": audience_id}, "context": {"workspaceId": workspace_id}},
    )
    if not audiences:
        raise RuntimeError(f"Audience {audience_id} not found in workspace {workspace_id}")
    return audiences[0]


def load_connections_by_name(api_url: str, token: str, workspace_id: str) -> dict[str, str]:
    connections = api_post(
        f"{api_url}/api/connections/query",
        token,
        {"content": {}, "context": {"workspaceId": workspace_id}},
    )
    return {connection["name"]: connection["id"] for connection in connections}


def resolve_treatment_query_source_id(
    connections_by_name: dict[str, str],
    audience_source_id: str,
    audience_source_name: str | None,
) -> tuple[str, str | None]:
    """Return (source_id, source_name) to use when querying treatments."""
    resolved_name = audience_source_name
    if not resolved_name:
        resolved_name = next(
            (
                name
                for name, connection_id in connections_by_name.items()
                if connection_id == audience_source_id
            ),
            None,
        )
    if resolved_name != GOOGLE_ANALYTICS_SOURCE_NAME:
        return audience_source_id, resolved_name

    adwords_source_id = connections_by_name.get(GOOGLE_ADWORDS_SOURCE_NAME)
    if not adwords_source_id:
        adwords_source_id = GOOGLE_ADWORDS_TREATMENT_SOURCE_ID
        logging.warning(
            "No %s connection in workspace; falling back to %s for treatment query",
            GOOGLE_ADWORDS_SOURCE_NAME,
            adwords_source_id,
        )
    return adwords_source_id, GOOGLE_ADWORDS_SOURCE_NAME


def query_treatments_by_source(
    api_url: str,
    token: str,
    workspace_id: str,
    source_id: str,
    cache: dict[tuple[str, str], list[dict]],
) -> list[dict]:
    cache_key = (workspace_id, source_id)
    if cache_key not in cache:
        cache[cache_key] = query_all_pages(
            f"{api_url}/api/treatments/query",
            token,
            workspace_id,
            {"source": source_id},
        )
    return cache[cache_key]


def build_audience_update_payload(
    workspace_id: str,
    audience_id: str,
    treatments: list[str],
) -> dict:
    return {
        "content": {
            "id": audience_id,
            "config": {
                "treatments": treatments,
            },
        },
        "context": {"workspaceId": workspace_id},
    }


def build_fix_plan(
    api_url: str,
    token: str,
    manual_dict: dict,
    candidate_rows: pd.DataFrame,
) -> list[dict]:
    plans: list[dict] = []
    treatment_cache: dict[tuple[str, str], list[dict]] = {}
    connections_cache: dict[str, dict[str, str]] = {}

    for _, row in candidate_rows.iterrows():
        workspace_name = row["workspace.name"]
        workspace_id = row["workspace.id"]
        audience_id = row["audience.id"]
        audience_name = row["audience.name"]
        workspace_config = manual_dict.get(workspace_name)
        if not workspace_config:
            continue

        customer_key = match_customer_key(audience_name, workspace_config)
        if customer_key is None:
            logging.info(
                "Skipping %s / %s: no customer key match in manual dict",
                workspace_name,
                audience_name,
            )
            continue

        customer_config = workspace_config[customer_key]
        audience = fetch_audience(api_url, token, workspace_id, audience_id)
        source_id = audience.get("connection") or audience.get("source")
        if not source_id:
            logging.warning(
                "Skipping %s / %s: audience has no source/connection",
                workspace_name,
                audience_name,
            )
            continue

        if workspace_id not in connections_cache:
            connections_cache[workspace_id] = load_connections_by_name(
                api_url, token, workspace_id
            )
        source_name = row.get("audience.source")
        if pd.isna(source_name):
            source_name = None
        source_name_from_id = next(
            (
                name
                for name, connection_id in connections_cache[workspace_id].items()
                if connection_id == source_id
            ),
            source_id,
        )
        audience_source_name = source_name or source_name_from_id
        treatment_query_source_id, treatment_query_source_name = (
            resolve_treatment_query_source_id(
                connections_cache[workspace_id],
                source_id,
                audience_source_name,
            )
        )
        if treatment_query_source_id != source_id:
            logging.info(
                "Querying treatments for %s / %s via %s (%s) instead of %s (%s)",
                workspace_name,
                audience_name,
                treatment_query_source_name,
                treatment_query_source_id,
                audience_source_name,
                source_id,
            )

        treatments_for_source = query_treatments_by_source(
            api_url,
            token,
            workspace_id,
            treatment_query_source_id,
            treatment_cache,
        )
        matched_treatments = select_matched_treatments(
            treatments_for_source, customer_config
        )
        matched_treatment_ids = [
            treatment["treatment.id"] for treatment in matched_treatments
        ]
        existing_treatment_ids = list(audience.get("config", {}).get("treatments") or [])
        merged_treatment_ids = merge_treatment_ids(
            existing_treatment_ids, matched_treatment_ids
        )
        added_treatment_ids = [
            treatment_id
            for treatment_id in merged_treatment_ids
            if treatment_id not in existing_treatment_ids
        ]
        added_treatments = [
            treatment
            for treatment in matched_treatments
            if treatment["treatment.id"] in added_treatment_ids
        ]

        plans.append(
            {
                "workspace_name": workspace_name,
                "workspace_id": workspace_id,
                "audience_id": audience_id,
                "audience_name": audience_name,
                "customer_key": customer_key,
                "source": audience_source_name,
                "source_id": source_id,
                "treatment_query_source": treatment_query_source_name,
                "treatment_query_source_id": treatment_query_source_id,
                "campaign_names": customer_config.get("campaign_names") or [],
                "campaign_regex": customer_config.get("campaign_regex"),
                "matched_treatment_ids": matched_treatment_ids,
                "existing_treatment_ids": existing_treatment_ids,
                "added_treatment_ids": added_treatment_ids,
                "added_treatments": added_treatments,
                "target_treatment_ids": merged_treatment_ids,
                "audience_update_payload": build_audience_update_payload(
                    workspace_id,
                    audience_id,
                    merged_treatment_ids,
                ),
            }
        )

    return plans


def print_plan_summary(plans: list[dict]) -> None:
    actionable = [plan for plan in plans if plan["added_treatment_ids"]]
    skipped = [plan for plan in plans if not plan["added_treatment_ids"]]

    logging.info("Found %s audience plan(s).", len(plans))
    logging.info("%s audience(s) would receive new treatments.", len(actionable))
    logging.info("%s audience(s) have no new treatments to add.", len(skipped))

    for index, plan in enumerate(plans, start=1):
        logging.info("")
        logging.info(
            "%s. %s | %s | %s",
            index,
            plan["workspace_name"],
            plan["audience_id"],
            plan["audience_name"],
        )
        logging.info("   Customer key: %s", plan["customer_key"])
        logging.info("   Source: %s (%s)", plan["source"], plan["source_id"])
        if plan["treatment_query_source_id"] != plan["source_id"]:
            logging.info(
                "   Treatment query source: %s (%s)",
                plan["treatment_query_source"],
                plan["treatment_query_source_id"],
            )
        logging.info(
            "   Campaign filter: names=%s regex=%s",
            json.dumps(plan["campaign_names"]),
            plan["campaign_regex"],
        )
        logging.info("   Existing treatments: %s", len(plan["existing_treatment_ids"]))
        logging.info("   Matched treatments: %s", len(plan["matched_treatment_ids"]))
        logging.info("   Added treatments: %s", plan["added_treatment_ids"])
        for treatment in plan["added_treatments"]:
            logging.info(
                "     %s -> %s",
                treatment["treatment.id"],
                treatment["treatment.campaign.name"],
            )
        if not plan["added_treatment_ids"]:
            logging.info("   Action: skip (nothing to add)")
        else:
            logging.info("   Target treatments: %s", plan["target_treatment_ids"])


def apply_plans(api_url: str, token: str, plans: list[dict]) -> None:
    applied = 0
    for plan in plans:
        if not plan["added_treatment_ids"]:
            logging.info(
                "Skipping %s (%s): no new treatments to add",
                plan["audience_name"],
                plan["audience_id"],
            )
            continue
        logging.info(
            "Applying audience update for %s (%s)...",
            plan["audience_name"],
            plan["audience_id"],
        )
        logging.info(
            "Adding treatments: %s",
            plan["added_treatment_ids"],
        )
        for treatment in plan["added_treatments"]:
            logging.info(
                "  %s -> %s",
                treatment["treatment.id"],
                treatment["treatment.campaign.name"],
            )
        api_post_data(api_url, token, "api/audiences/update", plan["audience_update_payload"])
        applied += 1
        logging.info(
            "Updated audience %s: added %s treatment(s)",
            plan["audience_id"],
            len(plan["added_treatment_ids"]),
        )
    logging.info("Applied updates to %s audience(s).", applied)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Add treatments to audiences missing them using "
            "manual_dict_add_treatments.json."
        )
    )
    parser.add_argument(
        "--manual-dict",
        type=Path,
        default=MANUAL_DICT_PATH,
        help="Path to manual_dict_add_treatments.json",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=AUDIT_CSV_PATH,
        help="Path to audience_model_treatments_audit.csv",
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
        help="Apply changes to the API",
    )
    parser.set_defaults(apply=False)
    parser.add_argument(
        "--plan-output",
        type=Path,
        default=PLAN_OUTPUT_PATH,
        help="Where to write the JSON plan of planned changes",
    )
    parser.add_argument(
        "--workspace",
        action="append",
        dest="workspaces",
        metavar="NAME",
        help=(
            "Limit to one or more workspace names from the manual dict. "
            "Repeat the flag or pass comma-separated names."
        ),
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path (default: fix_add_treatments_<timestamp>.log)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logging.info("Logging to %s", log_path)
    load_dotenv(SCRIPT_DIR / ".env")

    api_url = os.environ["TARGETING_URL"].rstrip("/")
    token = os.environ["API_SERVICE_TOKEN"]

    manual_dict = load_manual_dict(args.manual_dict)
    workspace_filter = normalize_workspace_filter(args.workspaces)
    if workspace_filter:
        unknown = workspace_filter - set(manual_dict)
        if unknown:
            raise SystemExit(
                "Workspace(s) not in manual dict: "
                + ", ".join(sorted(unknown))
            )
        workspace_names = workspace_filter
        logging.info("Workspace filter: %s", ", ".join(sorted(workspace_names)))
    else:
        workspace_names = set(manual_dict)
        logging.info(
            "Processing all configured workspaces: %s",
            ", ".join(sorted(workspace_names)),
        )

    candidate_rows = load_add_treatment_rows(args.csv, workspace_names)
    if candidate_rows.empty:
        logging.info(
            "No audiences with label %r found for configured workspace(s) in %s.",
            LABEL_ADD_TREATMENTS,
            args.csv,
        )
        return

    plans = build_fix_plan(api_url, token, manual_dict, candidate_rows)

    args.plan_output.parent.mkdir(parents=True, exist_ok=True)
    args.plan_output.write_text(
        json.dumps(plans, indent=2, default=str),
        encoding="utf-8",
    )
    logging.info("Wrote plan to %s", args.plan_output)

    if args.apply:
        logging.info("APPLY mode: writing changes to production.")
        apply_plans(api_url, token, plans)
    else:
        logging.info("DRY-RUN mode: no production changes were made.")
        print_plan_summary(plans)
        logging.info("Re-run with --apply to execute these changes.")


if __name__ == "__main__":
    main()
