"""
Fix incorrect Innkeepr - 30d Visitors/Visitor - Exclusion configurations.

Reads the generated audit CSV, selects incorrect 30d visitor exclusions, and
updates:
  - api/models/store (audienceSizePercentage, targetingOutlookDays)
  - api/audiences/update (model id, targetingOutlookDays)
  - innkeepr-analytics/configs/customer_specifications.yaml (exclude_visitors)

Dry-run is the default. Pass --apply to write changes to production.
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
import yaml
from dotenv import load_dotenv

from check_signal_configuration import (
    CUSTOMER_SPECS_PATH,
    EXCLUSION_NAME_OVERRIDES,
    SCRIPT_DIR,
    api_post,
    fetch_model,
    normalize_exclusion_overrides,
    sanitize_workspace_name,
)

TARGET_EXCLUSION_NAME = "Innkeepr - 30d Visitors - Exclusion"
TARGET_EXCLUSION_ALIASES = (
    TARGET_EXCLUSION_NAME,
    "Innkeepr - 30d Visitor - Exclusion",
)
AUDIT_CSV_PATH = SCRIPT_DIR / "signal_configuration_audit.csv"
PLAN_OUTPUT_PATH = SCRIPT_DIR / "fix_30d_visitors_exclusion_plan.json"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return SCRIPT_DIR / f"fix_30d_visitors_exclusion_{stamp}.log"


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


def matches_30d_visitors_exclusion(audience_name: str) -> bool:
    for name in TARGET_EXCLUSION_ALIASES:
        if audience_name == name or audience_name.startswith(f"{name} -"):
            return True
    return False


def get_target_defaults() -> dict:
    return normalize_exclusion_overrides(EXCLUSION_NAME_OVERRIDES[TARGET_EXCLUSION_NAME])


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


def load_incorrect_30d_visitors(
    table: pd.DataFrame,
    workspaces: set[str] | None = None,
) -> pd.DataFrame:
    mask = table["audience.name"].apply(matches_30d_visitors_exclusion)
    incorrect = table[mask & (table["label"] == False)].copy()  # noqa: E712
    incorrect = filter_by_workspaces(incorrect, workspaces)
    return incorrect.sort_values(["workspace.name", "audience.name"]).reset_index(drop=True)


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


def load_workspace_map(api_url: str, token: str) -> dict[str, str]:
    workspaces = api_post(f"{api_url}/api/core/workspaces/query", token, {"content": {}})
    return {workspace["name"]: workspace["id"] for workspace in workspaces}


def fetch_audience(api_url: str, token: str, workspace_id: str, audience_id: str) -> dict:
    audiences = api_post(
        f"{api_url}/api/audiences/query",
        token,
        {"content": {"id": audience_id}, "context": {"workspaceId": workspace_id}},
    )
    if not audiences:
        raise RuntimeError(f"Audience {audience_id} not found in workspace {workspace_id}")
    return audiences[0]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_model_store_payload(
    workspace_id: str,
    audience: dict,
    model: dict,
    target_defaults: dict,
) -> dict:
    config = audience.get("config", {})
    return {
        "content": {
            "path": model["path"],
            "created": utc_now_iso(),
            "goal": model["goal"],
            "treatment": model.get("treatment") or config.get("treatments") or [],
            "type": model["type"],
            "audienceSize": model.get("audienceSize"),
            "audience": audience["id"],
            "audienceSizePercentage": target_defaults["audienceSizePercentage"],
            "treatmentSessionCount": model["treatmentSessionCount"],
            "targetingOutlookDays": target_defaults["targetingOutlookDays"],
            "trainingOutlookDays": model.get("trainingOutlookDays"),
            "f1Score": model["f1Score"],
            "conversionLag": model.get("conversionLag"),
            "manualRetrainReason": model.get("manualRetrainReason"),
            "treatmentSyncStrategy": config.get("treatmentSyncStrategy"),
        },
        "context": {"workspaceId": workspace_id},
    }


def build_audience_update_payload(
    workspace_id: str,
    audience_id: str,
    model_id: str,
    target_defaults: dict,
) -> dict:
    return {
        "content": {
            "id": audience_id,
            "status": "active",
            "config": {
                "model": model_id,
                "targetingOutlookDays": target_defaults["targetingOutlookDays"],
            },
        },
        "context": {"workspaceId": workspace_id},
    }


def get_yaml_exclude_visitors(customer_specs: dict, workspace_name: str, audience_id: str):
    workspace_key = sanitize_workspace_name(workspace_name)
    workspace_specs = customer_specs.get(workspace_key, {})
    if not isinstance(workspace_specs, dict):
        return workspace_key, None
    audience_specs = workspace_specs.get(audience_id, {})
    if not isinstance(audience_specs, dict):
        return workspace_key, None
    return workspace_key, audience_specs.get("exclude_visitors")


def plan_yaml_change(
    customer_specs: dict,
    workspace_name: str,
    audience_id: str,
    target_defaults: dict,
) -> dict | None:
    workspace_key, current_exclude = get_yaml_exclude_visitors(
        customer_specs, workspace_name, audience_id
    )
    target_exclude = target_defaults.get("exclude_visitors")
    if target_exclude is None and current_exclude is None:
        return None
    if target_exclude is not None and current_exclude == target_exclude:
        return None
    return {
        "workspace_key": workspace_key,
        "audience_id": audience_id,
        "current_exclude_visitors": current_exclude,
        "target_exclude_visitors": target_exclude,
        "action": "remove_key" if target_exclude is None else "set_value",
    }


def apply_yaml_changes(customer_specs: dict, yaml_changes: list[dict]) -> int:
    applied = 0
    for change in yaml_changes:
        workspace_key = change["workspace_key"]
        audience_id = change["audience_id"]
        workspace_specs = customer_specs.setdefault(workspace_key, {})
        audience_specs = workspace_specs.setdefault(audience_id, {})

        if change["action"] == "remove_key":
            if "exclude_visitors" in audience_specs:
                del audience_specs["exclude_visitors"]
                applied += 1
            if not audience_specs:
                del workspace_specs[audience_id]
        else:
            audience_specs["exclude_visitors"] = change["target_exclude_visitors"]
            applied += 1
    return applied


def build_fix_plan(
    api_url: str,
    token: str,
    incorrect_rows: pd.DataFrame,
    customer_specs: dict,
    target_defaults: dict,
) -> list[dict]:
    workspace_map = load_workspace_map(api_url, token)
    plans: list[dict] = []

    for _, row in incorrect_rows.iterrows():
        workspace_name = row["workspace.name"]
        audience_id = row["audience.id"]
        audience_name = row["audience.name"]
        workspace_id = workspace_map.get(workspace_name)
        if not workspace_id:
            raise RuntimeError(f"Workspace id not found for {workspace_name}")

        audience = fetch_audience(api_url, token, workspace_id, audience_id)
        model_id = audience.get("config", {}).get("model")
        model = fetch_model(api_url, token, workspace_id, model_id)
        if not model:
            raise RuntimeError(f"Model not found for audience {audience_id}")

        yaml_change = plan_yaml_change(
            customer_specs, workspace_name, audience_id, target_defaults
        )
        model_payload = build_model_store_payload(
            workspace_id, audience, model, target_defaults
        )

        plans.append(
            {
                "workspace_name": workspace_name,
                "workspace_id": workspace_id,
                "audience_id": audience_id,
                "audience_name": audience_name,
                "audit_comment": row["comment"] if pd.notna(row["comment"]) else "",
                "current": {
                    "targetingOutlookDays": audience.get("config", {}).get(
                        "targetingOutlookDays"
                    ),
                    "model_targetingOutlookDays": model.get("targetingOutlookDays"),
                    "audienceSizePercentage": model.get("audienceSizePercentage"),
                    "exclude_visitors_yaml": get_yaml_exclude_visitors(
                        customer_specs, workspace_name, audience_id
                    )[1],
                },
                "target": {
                    "targetingOutlookDays": target_defaults["targetingOutlookDays"],
                    "audienceSizePercentage": target_defaults["audienceSizePercentage"],
                    "exclude_visitors_yaml": target_defaults.get("exclude_visitors"),
                },
                "model_store_payload": model_payload,
                "yaml_change": yaml_change,
                "current_model_id": model_id,
            }
        )

    return plans


def store_model(api_url: str, token: str, payload: dict) -> dict:
    data = api_post_data(api_url, token, "api/models/store", payload)
    if isinstance(data, list):
        if not data:
            raise RuntimeError("models/store returned empty data")
        return data[0]
    return data


def update_audience(api_url: str, token: str, payload: dict) -> dict:
    return api_post_data(api_url, token, "api/audiences/update", payload)


def print_plan_summary(plans: list[dict], target_defaults: dict) -> None:
    logging.info("Target defaults for %s:", TARGET_EXCLUSION_NAME)
    logging.info("  targetingOutlookDays: %s", target_defaults["targetingOutlookDays"])
    logging.info(
        "  audienceSizePercentage: %s", target_defaults["audienceSizePercentage"]
    )
    logging.info("  exclude_visitors (yaml): %s", target_defaults.get("exclude_visitors"))
    logging.info("Found %s incorrect audiences to fix.", len(plans))

    for index, plan in enumerate(plans, start=1):
        logging.info("")
        logging.info(
            "%s. %s | %s | %s",
            index,
            plan["workspace_name"],
            plan["audience_id"],
            plan["audience_name"],
        )
        logging.info("   Audit comment: %s", plan["audit_comment"] or "-")
        logging.info("   Current: %s", json.dumps(plan["current"], default=str))
        logging.info("   Target:  %s", json.dumps(plan["target"], default=str))
        if plan["yaml_change"]:
            logging.info("   YAML:    %s", json.dumps(plan["yaml_change"], default=str))
        else:
            logging.info("   YAML:    no change")


def apply_plans(
    api_url: str,
    token: str,
    plans: list[dict],
    customer_specs: dict,
    target_defaults: dict,
) -> None:
    yaml_changes: list[dict] = []

    for plan in plans:
        logging.info(
            "Applying API updates for %s (%s)...",
            plan["audience_name"],
            plan["audience_id"],
        )
        stored_model = store_model(api_url, token, plan["model_store_payload"])
        new_model_id = stored_model["id"]
        audience_payload = build_audience_update_payload(
            plan["workspace_id"],
            plan["audience_id"],
            new_model_id,
            target_defaults,
        )
        update_audience(api_url, token, audience_payload)
        logging.info(
            "Updated model %s -> %s and audience %s",
            plan["current_model_id"],
            new_model_id,
            plan["audience_id"],
        )
        if plan["yaml_change"]:
            yaml_changes.append(plan["yaml_change"])

    if yaml_changes:
        applied = apply_yaml_changes(customer_specs, yaml_changes)
        with CUSTOMER_SPECS_PATH.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(
                customer_specs,
                handle,
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=True,
            )
        logging.info(
            "Updated customer_specifications.yaml (%s audience entries changed).",
            applied,
        )
    else:
        logging.info("No customer_specifications.yaml changes required.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fix incorrect Innkeepr - 30d Visitors/Visitor - Exclusion configurations "
            "from the generated audit CSV."
        )
    )
    parser.add_argument(
        "--audit-csv",
        type=Path,
        default=AUDIT_CSV_PATH,
        help="Path to signal_configuration_audit.csv",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes to API and customer_specifications.yaml (default: dry-run)",
    )
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
            "comma-separated names, e.g. --workspace Junglueck --workspace More"
        ),
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path (default: fix_30d_visitors_exclusion_<timestamp>.log)",
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
    target_defaults = get_target_defaults()

    workspace_filter = normalize_workspace_filter(args.workspaces)
    table = pd.read_csv(args.audit_csv)
    incorrect_rows = load_incorrect_30d_visitors(table, workspace_filter)
    if incorrect_rows.empty:
        if workspace_filter:
            logging.info(
                "No incorrect %s audiences found for workspace(s) %s in %s.",
                TARGET_EXCLUSION_NAME,
                ", ".join(sorted(workspace_filter)),
                args.audit_csv,
            )
        else:
            logging.info(
                "No incorrect %s audiences found in %s.",
                TARGET_EXCLUSION_NAME,
                args.audit_csv,
            )
        return

    if workspace_filter:
        logging.info(
            "Workspace filter: %s",
            ", ".join(sorted(workspace_filter)),
        )

    customer_specs = yaml.safe_load(CUSTOMER_SPECS_PATH.read_text(encoding="utf-8"))
    plans = build_fix_plan(api_url, token, incorrect_rows, customer_specs, target_defaults)

    serializable_plans = []
    for plan in plans:
        serializable = dict(plan)
        serializable["audience_update_payload_preview"] = build_audience_update_payload(
            plan["workspace_id"],
            plan["audience_id"],
            "<new_model_id>",
            target_defaults,
        )
        serializable_plans.append(serializable)

    args.plan_output.write_text(
        json.dumps(serializable_plans, indent=2, default=str),
        encoding="utf-8",
    )
    logging.info("Wrote plan to %s", args.plan_output)

    if args.apply:
        logging.info("APPLY mode: writing changes to production.")
        apply_plans(api_url, token, plans, customer_specs, target_defaults)
        logging.info("Apply completed for %s audiences.", len(plans))
    else:
        logging.info("DRY-RUN mode: no production changes were made.")
        print_plan_summary(plans, target_defaults)
        logging.info("Re-run with --apply to execute these changes.")


if __name__ == "__main__":
    main()
