"""
Fix audiences with treatmentSyncStrategy != campaignBased.

Reads audience_model_treatments_causal_results.csv, filters rows where
audience.treatmentSyncStrategy is not campaignBased, and updates each audience
via api/audiences/update with:

  { "id": <audience.id>, "config": { "treatmentSyncStrategy": "campaignBased" } }

Use --dry-run to preview changes (default) or --apply to write to production.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

from check_signal_configuration import api_post
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR,
    FIX_TREATMENT_SYNC_STRATEGY_DATA_DIR,
    FIX_TREATMENT_SYNC_STRATEGY_LOGS_DIR as LOGS_DIR,
    SCRIPT_DIR,
)

CAUSAL_RESULTS_CSV_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_causal_results.csv"
)
PLAN_OUTPUT_PATH = (
    FIX_TREATMENT_SYNC_STRATEGY_DATA_DIR / "fix_treatment_sync_strategy_plan.json"
)
TARGET_TREATMENT_SYNC_STRATEGY = "campaignBased"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"fix_treatment_sync_strategy_{stamp}.log"


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
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
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


def filter_by_workspaces(
    table: pd.DataFrame, workspaces: set[str] | None
) -> pd.DataFrame:
    if not workspaces:
        return table
    filtered = table[table["workspace.name"].isin(workspaces)].copy()
    missing = workspaces - set(filtered["workspace.name"].unique())
    if missing:
        logging.warning(
            "No matching audiences for workspace(s): %s",
            ", ".join(sorted(missing)),
        )
    return filtered


def load_wrong_treatment_sync_rows(
    table: pd.DataFrame,
    workspaces: set[str] | None = None,
) -> pd.DataFrame:
    strategy = table["audience.treatmentSyncStrategy"].fillna("")
    wrong = table[strategy != TARGET_TREATMENT_SYNC_STRATEGY].copy()
    wrong = filter_by_workspaces(wrong, workspaces)
    return wrong.sort_values(["workspace.name", "audience.name"]).reset_index(drop=True)


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


def build_audience_update_payload(workspace_id: str, audience_id: str) -> dict:
    return {
        "content": {
            "id": audience_id,
            "config": {
                "treatmentSyncStrategy": TARGET_TREATMENT_SYNC_STRATEGY,
            },
        },
        "context": {"workspaceId": workspace_id},
    }


def build_fix_plan(
    api_url: str,
    token: str,
    wrong_rows: pd.DataFrame,
) -> list[dict]:
    plans: list[dict] = []

    for _, row in wrong_rows.iterrows():
        workspace_name = row["workspace.name"]
        workspace_id = row["workspace.id"]
        audience_id = row["audience.id"]
        audience_name = row["audience.name"]
        csv_strategy = row["audience.treatmentSyncStrategy"]

        audience = fetch_audience(api_url, token, workspace_id, audience_id)
        current_strategy = audience.get("config", {}).get("treatmentSyncStrategy")
        status = audience.get("status", "active")

        plans.append(
            {
                "workspace_name": workspace_name,
                "workspace_id": workspace_id,
                "audience_id": audience_id,
                "audience_name": audience_name,
                "label": row["label"] if pd.notna(row.get("label")) else "",
                "current": {
                    "treatmentSyncStrategy_csv": csv_strategy,
                    "treatmentSyncStrategy_api": current_strategy,
                    "status": status,
                },
                "target": {
                    "treatmentSyncStrategy": TARGET_TREATMENT_SYNC_STRATEGY,
                },
                "audience_update_payload": build_audience_update_payload(
                    workspace_id, audience_id
                ),
            }
        )

    return plans


def update_audience(api_url: str, token: str, payload: dict) -> dict:
    return api_post_data(api_url, token, "api/audiences/update", payload)


def print_plan_summary(plans: list[dict]) -> None:
    strategy_counts: dict[str, int] = {}
    for plan in plans:
        current = plan["current"]["treatmentSyncStrategy_api"]
        key = current if current else "<missing>"
        strategy_counts[key] = strategy_counts.get(key, 0) + 1

    logging.info(
        "Target treatmentSyncStrategy: %s",
        TARGET_TREATMENT_SYNC_STRATEGY,
    )
    logging.info("Found %s audiences to fix.", len(plans))
    logging.info(
        "Current strategies: %s",
        ", ".join(f"{strategy}={count}" for strategy, count in sorted(strategy_counts.items())),
    )

    for index, plan in enumerate(plans, start=1):
        logging.info("")
        logging.info(
            "%s. %s | %s | %s",
            index,
            plan["workspace_name"],
            plan["audience_id"],
            plan["audience_name"],
        )
        logging.info("   Label: %s", plan["label"] or "-")
        logging.info("   Current: %s", json.dumps(plan["current"], default=str))
        logging.info("   Target:  %s", json.dumps(plan["target"], default=str))


def apply_plans(api_url: str, token: str, plans: list[dict]) -> None:
    for plan in plans:
        logging.info(
            "Applying audience update for %s (%s)...",
            plan["audience_name"],
            plan["audience_id"],
        )
        update_audience(api_url, token, plan["audience_update_payload"])
        logging.info(
            "Updated audience %s: treatmentSyncStrategy -> %s",
            plan["audience_id"],
            TARGET_TREATMENT_SYNC_STRATEGY,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fix audiences with treatmentSyncStrategy != campaignBased "
            "from audience_model_treatments_causal_results.csv."
        )
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=CAUSAL_RESULTS_CSV_PATH,
        help="Path to audience_model_treatments_causal_results.csv",
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
            "Limit to one or more workspace names. Repeat the flag or pass "
            "comma-separated names, e.g. --workspace Tchibo --workspace More"
        ),
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path (default: fix_treatment_sync_strategy_<timestamp>.log)",
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

    workspace_filter = normalize_workspace_filter(args.workspaces)
    table = pd.read_csv(args.csv)
    wrong_rows = load_wrong_treatment_sync_rows(table, workspace_filter)
    if wrong_rows.empty:
        if workspace_filter:
            logging.info(
                "No audiences with wrong treatmentSyncStrategy found for "
                "workspace(s) %s in %s.",
                ", ".join(sorted(workspace_filter)),
                args.csv,
            )
        else:
            logging.info(
                "No audiences with wrong treatmentSyncStrategy found in %s.",
                args.csv,
            )
        return

    if workspace_filter:
        logging.info(
            "Workspace filter: %s",
            ", ".join(sorted(workspace_filter)),
        )

    plans = build_fix_plan(api_url, token, wrong_rows)

    args.plan_output.parent.mkdir(parents=True, exist_ok=True)
    args.plan_output.write_text(
        json.dumps(plans, indent=2, default=str),
        encoding="utf-8",
    )
    logging.info("Wrote plan to %s", args.plan_output)

    if args.apply:
        logging.info("APPLY mode: writing changes to production.")
        apply_plans(api_url, token, plans)
        logging.info("Apply completed for %s audiences.", len(plans))
    else:
        logging.info("DRY-RUN mode: no production changes were made.")
        print_plan_summary(plans)
        logging.info("Re-run with --apply (or without --dry-run) to execute these changes.")


if __name__ == "__main__":
    main()
