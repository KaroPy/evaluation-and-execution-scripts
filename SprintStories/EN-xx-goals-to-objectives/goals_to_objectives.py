"""
Backfill signal.objective and model.objective, and conversion model paths.

For each workspace:
  1. Query objectives (objectives/query)
  2. Query signals (signals/query) and load each signal's configured model
  3. If signal.objective is missing but model.objective is set, backfill the
     signal from the model
  4. If signal.objective is set, sync model.objective from signal.objective
  5. For conversion models, swap goal.id with objective.id in model.path when needed
  6. Check innkeepr-targeting-<sanitized_tenant> for the target path in S3; if missing,
     copy the most recent model, coding, and config files from the goal-based path
  7. Store an updated model via models/store and point the signal at it

Dry-run is the default. Pass --apply to write changes.

Usage (from repo root):
    python SprintStories/EN-xx-goals-to-objectives/goals_to_objectives.py
    python SprintStories/EN-xx-goals-to-objectives/goals_to_objectives.py --customer Rosental
    python SprintStories/EN-xx-goals-to-objectives/goals_to_objectives.py --apply --customer Rosental
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.call_api_with_account_id import (  # noqa: E402
    call_api_with_accountId,
    send_to_innkeepr_api_paginated,
)
from general_functions.conncet_s3 import S3Connection  # noqa: E402
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402
from general_functions.sanitize_accout_name import sanitize_account_name  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_PLAN_OUTPUT = DATA_DIR / f"goals_to_objectives_plan_{datetime.now()}.csv"
DEFAULT_OBJECTIVES_OUTPUT = DATA_DIR / f"objectives_extract_{datetime.now()}.csv"

DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
MODEL_FOLDER_MARKERS = (
    "_best_models",
    "_new_models",
    "_best_models_lstm",
    "_best_models_likely",
)


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"goals_to_objectives_{stamp}.log"


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


def normalize_events(value: object) -> tuple[str, ...]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ()
    if isinstance(value, list):
        return tuple(sorted(str(item) for item in value if item is not None and str(item)))
    text = str(value).strip()
    return (text,) if text else ()


def utc_now_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def query_all(endpoint_url: str, account_id: str, content: dict, logger: logging.Logger) -> list:
    return send_to_innkeepr_api_paginated(endpoint_url, account_id, content, logger)


def targeting_bucket(workspace_name: str) -> str:
    return f"innkeepr-targeting-{sanitize_account_name(workspace_name)}"


def conversion_root_prefix(workspace_name: str, goal_id: str) -> str:
    return f"{sanitize_account_name(workspace_name)}-conversion-{goal_id}/"


def swap_goal_id_in_path(path: str, goal_id: str, objective_id: str) -> str:
    if not path or not goal_id or not objective_id or goal_id == objective_id:
        return path
    return path.replace(goal_id, objective_id)


def coding_path_from_model_path(path: str) -> str:
    return (
        path.replace("_best_models/", "-coding/")
        .replace("_best_models_lstm/", "-coding_models_lstm/")
        .replace("_best_models_likely/", "-coding_models_likely/")
        .replace("_new_models/", "-coding/")
    )


def configs_prefix_from_model_path(path: str) -> str:
    if "best_models_lstm" in path or "new_models_lstm" in path:
        configs_dir = "configs_models_lstm"
    elif "best_models_likely" in path or "new_models_likely" in path:
        configs_dir = "configs_models_likely"
    else:
        configs_dir = "configs"
    return f"{path.split('/')[0]}/{configs_dir}/"


def prefix_has_files(s3: S3Connection, bucket: str, prefix: str) -> bool:
    if not prefix:
        return False
    files = s3.list_files_with_pagination(bucket, prefix)
    return any(file_key != prefix.rstrip("/") for file_key in files)


def find_most_recent_conversion_model_prefix(
    s3: S3Connection,
    bucket: str,
    workspace_name: str,
    goal_id: str,
) -> str | None:
    root = conversion_root_prefix(workspace_name, goal_id)
    prefixes = s3.list_files_with_pagination(bucket, root, delimiter="/")
    candidates: list[tuple[str, str]] = []
    for prefix in prefixes:
        if not any(marker in prefix for marker in MODEL_FOLDER_MARKERS):
            continue
        dates = DATE_PATTERN.findall(prefix)
        if dates:
            candidates.append((dates[0], prefix))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def resolve_source_model_prefix(
    s3: S3Connection,
    bucket: str,
    workspace_name: str,
    goal_id: str,
    model_path: str | None,
) -> str | None:
    if model_path and prefix_has_files(s3, bucket, model_path):
        return model_path.rstrip("/")
    return find_most_recent_conversion_model_prefix(s3, bucket, workspace_name, goal_id)


def copy_conversion_artifacts(
    s3: S3Connection,
    bucket: str,
    source_model_prefix: str,
    dest_model_prefix: str,
    logger: logging.Logger,
) -> None:
    source_model = source_model_prefix
    dest_model = dest_model_prefix
    logger.info("Copying model files %s -> %s", source_model, dest_model)
    s3.copy_all_files_recursively(bucket, source_model, dest_model)

    source_coding = coding_path_from_model_path(source_model)
    dest_coding = coding_path_from_model_path(dest_model)
    if source_coding != source_model and prefix_has_files(s3, bucket, source_coding):
        logger.info("Copying coding files %s -> %s", source_coding, dest_coding)
        s3.copy_all_files_recursively(bucket, source_coding, dest_coding)

    source_configs = configs_prefix_from_model_path(source_model)
    dest_configs = configs_prefix_from_model_path(dest_model)
    if prefix_has_files(s3, bucket, source_configs):
        logger.info("Copying config files %s -> %s", source_configs, dest_configs)
        s3.copy_all_files_recursively(bucket, source_configs, dest_configs)


def get_model(
    api_url: str,
    account_id: str,
    model_id: str,
    logger: logging.Logger,
) -> dict | None:
    models = call_api_with_accountId(
        f"{api_url}/api/models/query",
        account_id,
        {"id": model_id},
        logger,
    )
    return models[0] if models else None


def build_model_store_payload(
    model: dict,
    objective_id: str,
    signal: dict,
    target_path: str | None = None,
) -> dict:
    payload = model.copy()
    payload.pop("id", None)
    payload["objective"] = objective_id
    if target_path:
        payload["path"] = target_path
    if payload.get("f1Score") is None:
        payload.pop("f1Score", None)
    if not payload.get("scope"):
        treatments = (signal.get("config") or {}).get("treatments") or {}
        payload["scope"] = treatments.get("scope")
        if not payload["scope"]:
            payload.pop("scope")
    payload["created"] = utc_now_iso()
    print("Model payload:", payload)
    return payload


def store_model(
    api_url: str,
    account_id: str,
    payload: dict,
    logger: logging.Logger,
) -> str | None:
    result = call_api_with_accountId(f"{api_url}/api/models/store", account_id, payload, logger)
    if not result:
        return None
    stored = result[0] if isinstance(result, list) else result
    if isinstance(stored, dict):
        return stored.get("id")
    return None


def update_signal(
    api_url: str,
    account_id: str,
    signal_id: str,
    logger: logging.Logger,
    *,
    objective_id: str | None = None,
    model_id: str | None = None,
) -> None:
    payload: dict[str, str] = {"id": signal_id}
    if objective_id is not None:
        payload["objective"] = objective_id
    if model_id is not None:
        payload["model"] = model_id
    call_api_with_accountId(
        f"{api_url}/api/signals/update",
        account_id,
        payload,
        logger,
    )


def resolve_objective_context(signal: dict, model: dict) -> tuple[str | None, bool]:
    signal_objective = signal.get("objective")
    model_objective = model.get("objective")
    if signal_objective:
        return signal_objective, False
    if model_objective:
        return model_objective, True
    return None, False


def skip_plan_row(
    workspace_name: str,
    account_id: str,
    signal: dict,
    *,
    model_id: str | None = None,
    model: dict | None = None,
    reason: str,
) -> dict:
    signal_objective = signal.get("objective")
    return {
        "workspace.name": workspace_name,
        "workspace.id": account_id,
        "signal.id": signal.get("id"),
        "signal.name": signal.get("name"),
        "signal.status": signal.get("status"),
        "signal.objective": signal_objective,
        "signal.objective.target": signal_objective,
        "signal.objective.sync.needed": False,
        "model.id": model_id,
        "model.type": model.get("type") if model else None,
        "model.goal": model.get("goal") if model else None,
        "model.objective.current": model.get("objective") if model else None,
        "model.objective.target": signal_objective,
        "model.path.current": model.get("path") if model else None,
        "model.path.target": model.get("path") if model else None,
        "model.path.source.copy": None,
        "s3.bucket": None,
        "s3.target.exists": None,
        "s3.copy.needed": False,
        "objective.events": None,
        "action": "skip",
        "reason": reason,
    }


def extract_objectives_rows(
    workspace_name: str,
    workspace_id: str,
    objectives: list[dict],
) -> list[dict]:
    rows: list[dict] = []
    for objective in objectives:
        rows.append(
            {
                "workspace.name": workspace_name,
                "workspace.id": workspace_id,
                "objective.id": objective.get("id"),
                "objective.name": objective.get("name"),
                "objective.events": list(normalize_events(objective.get("events"))),
            }
        )
    return rows


def evaluate_signal_plan(
    workspace_name: str,
    signal: dict,
    model: dict,
    objectives_by_id: dict[str, dict],
    s3: S3Connection | None,
    logger: logging.Logger,
) -> dict:
    signal_id = signal.get("id")
    signal_name = signal.get("name")
    model_id = signal.get("model")
    signal_objective = signal.get("objective")
    goal_id = model.get("goal")
    objective_id, signal_objective_needs_sync = resolve_objective_context(signal, model)
    current_objective = model.get("objective")
    current_path = model.get("path") or ""
    model_type = model.get("type")

    plan = {
        "workspace.name": workspace_name,
        "signal.id": signal_id,
        "signal.name": signal_name,
        "signal.status": signal.get("status"),
        "signal.objective": signal_objective,
        "signal.objective.target": objective_id,
        "signal.objective.sync.needed": signal_objective_needs_sync,
        "model.id": model_id,
        "model.type": model_type,
        "model.goal": goal_id,
        "model.objective.current": current_objective,
        "model.objective.target": objective_id,
        "model.path.current": current_path,
        "model.path.target": current_path,
        "model.path.source.copy": None,
        "s3.bucket": None,
        "s3.target.exists": None,
        "s3.copy.needed": False,
        "objective.events": None,
        "action": "skip",
        "reason": "",
    }

    objective = objectives_by_id.get(objective_id) if objective_id else None
    if objective is not None:
        plan["objective.events"] = list(normalize_events(objective.get("events")))

    objective_needs_sync = current_objective != objective_id
    path_string_needs_update = False
    target_path = current_path
    if model_type == "conversion" and objective_id and current_path:
        if goal_id and goal_id != objective_id:
            target_path = swap_goal_id_in_path(current_path, goal_id, objective_id)
        path_string_needs_update = current_path != target_path
        plan["model.path.target"] = target_path
        plan["s3.bucket"] = targeting_bucket(workspace_name)

        if s3 is not None:
            bucket = plan["s3.bucket"]
            target_exists = prefix_has_files(s3, bucket, target_path)
            plan["s3.target.exists"] = target_exists
            if not target_exists:
                source_prefix = resolve_source_model_prefix(
                    s3,
                    bucket,
                    workspace_name,
                    goal_id or objective_id,
                    current_path,
                )
                plan["model.path.source.copy"] = source_prefix
                plan["s3.copy.needed"] = bool(source_prefix)
                if not source_prefix:
                    logger.warning(
                        "No source S3 prefix found for %s goal %s",
                        workspace_name,
                        goal_id or objective_id,
                    )

    if (
        not signal_objective_needs_sync
        and not objective_needs_sync
        and not path_string_needs_update
        and not plan["s3.copy.needed"]
    ):
        plan["reason"] = "already in sync"
        return plan

    reasons: list[str] = []
    if signal_objective_needs_sync:
        reasons.append("sync signal.objective from model.objective")
    if objective_needs_sync:
        reasons.append("sync model.objective from signal.objective")
    if path_string_needs_update:
        reasons.append("swap goal.id to objective.id in model.path")
    if plan["s3.copy.needed"]:
        reasons.append("copy S3 model/coding/config files to objective path")

    plan["action"] = "update"
    plan["reason"] = "; ".join(reasons)
    return plan


def build_workspace_plans(
    api_url: str,
    workspace: dict,
    s3: S3Connection | None,
    logger: logging.Logger,
) -> tuple[list[dict], list[dict]]:
    account_id = workspace["id"]
    workspace_name = workspace["name"]
    plans: list[dict] = []

    objectives = query_all(f"{api_url}/api/objectives/query", account_id, {}, logger)
    signals = query_all(f"{api_url}/api/signals/query", account_id, {}, logger)

    logger.info(
        "Workspace %s: %s objectives, %s signals",
        workspace_name,
        len(objectives),
        len(signals),
    )

    objectives_by_id = {
        objective["id"]: objective for objective in objectives if objective.get("id")
    }
    objective_rows = extract_objectives_rows(workspace_name, account_id, objectives)
    model_cache: dict[str, dict | None] = {}

    for signal in signals:
        signal_objective = signal.get("objective")
        model_id = signal.get("model")

        if not model_id:
            plans.append(
                skip_plan_row(
                    workspace_name,
                    account_id,
                    signal,
                    reason="signal has no model",
                )
            )
            continue

        if model_id not in model_cache:
            model_cache[model_id] = get_model(api_url, account_id, model_id, logger)
        model = model_cache[model_id]
        if model is None:
            plans.append(
                skip_plan_row(
                    workspace_name,
                    account_id,
                    signal,
                    model_id=model_id,
                    reason=f"model {model_id} not found",
                )
            )
            continue

        if not signal_objective and not model.get("objective"):
            plans.append(
                skip_plan_row(
                    workspace_name,
                    account_id,
                    signal,
                    model_id=model_id,
                    model=model,
                    reason="signal.objective not set",
                )
            )
            continue

        plan = evaluate_signal_plan(
            workspace_name,
            signal,
            model,
            objectives_by_id,
            s3,
            logger,
        )
        plan["workspace.id"] = account_id
        plans.append(plan)

    return plans, objective_rows


def print_plan_summary(plans: pd.DataFrame) -> None:
    action_counts = plans["action"].value_counts().to_dict()
    logging.info("Planned actions: %s", action_counts)
    to_update = plans[plans["action"] == "update"]
    logging.info("Signals to update: %s", len(to_update))

    for _, row in to_update.iterrows():
        logging.info(
            "%s | signal %s (%s) | model %s | signal_objective %s -> %s | path %s -> %s | s3_copy=%s",
            row["workspace.name"],
            row["signal.id"],
            row["signal.name"],
            row["model.id"],
            row["signal.objective"],
            row["signal.objective.target"],
            row["model.path.current"],
            row["model.path.target"],
            row["s3.copy.needed"],
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill signal.objective from model.objective and sync conversion model paths."
        )
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
        help="Apply changes via S3 copy, models/store, and signals/update",
    )
    parser.set_defaults(apply=False)
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --customer Rosental",
    )
    parser.add_argument(
        "--plan-output",
        type=Path,
        default=DEFAULT_PLAN_OUTPUT,
        help=f"CSV plan output path (default: {DEFAULT_PLAN_OUTPUT.name})",
    )
    parser.add_argument(
        "--objectives-output",
        type=Path,
        default=DEFAULT_OBJECTIVES_OUTPUT,
        help=f"CSV extract of objectives/query (default: {DEFAULT_OBJECTIVES_OUTPUT.name})",
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
        workspaces = [workspace for workspace in workspaces if workspace["name"] in customer_filter]
        missing = customer_filter - {workspace["name"] for workspace in workspaces}
        if missing:
            logger.warning("Workspace(s) not found: %s", ", ".join(sorted(missing)))

    if not workspaces:
        logger.info("No workspaces to process.")
        return

    try:
        s3 = S3Connection()
    except Exception as exc:
        s3 = None
        logger.warning("S3 connection unavailable (%s); path existence checks will be skipped", exc)

    all_plans: list[dict] = []
    all_objectives: list[dict] = []
    for workspace in workspaces:
        logger.info("=== Workspace: %s (%s) ===", workspace["name"], workspace["id"])
        plans, objective_rows = build_workspace_plans(api_url, workspace, s3, logger)
        all_plans.extend(plans)
        all_objectives.extend(objective_rows)
        pd.DataFrame(objective_rows).to_csv(f"{workspace['name']}_objectives.csv", index=False)
        pd.DataFrame(plans).to_csv(f"{workspace['name']}_plans.csv", index=False)

    objectives_df = pd.DataFrame(all_objectives)
    args.objectives_output.parent.mkdir(parents=True, exist_ok=True)
    objectives_df.to_csv(args.objectives_output, index=False)
    logger.info("Saved objectives extract to %s", args.objectives_output)

    plans = pd.DataFrame(all_plans)
    args.plan_output.parent.mkdir(parents=True, exist_ok=True)
    plans.to_csv(args.plan_output, index=False)
    logger.info("Saved plan to %s", args.plan_output)

    if plans.empty:
        logger.info("No signals found.")
        return

    print_plan_summary(plans)

    to_update = plans[plans["action"] == "update"]
    if to_update.empty:
        logger.info("Nothing to update.")
        return

    signals_by_id: dict[tuple[str, str], dict] = {}
    for workspace in workspaces:
        account_id = workspace["id"]
        for signal in query_all(f"{api_url}/api/signals/query", account_id, {}, logger):
            signal_id = signal.get("id")
            if signal_id:
                signals_by_id[account_id, signal_id] = signal

    updated = 0
    for _, row in to_update.iterrows():
        account_id = row["workspace.id"]
        workspace_name = row["workspace.name"]
        signal_id = row["signal.id"]
        model_id = row["model.id"]
        objective_id = row["model.objective.target"]
        target_path = row["model.path.target"]
        signal = signals_by_id.get((account_id, signal_id))
        if signal is None:
            logger.warning("Signal %s not found during apply — skipping", signal_id)
            continue

        model = get_model(api_url, account_id, model_id, logger)
        if model is None:
            logger.warning("Model %s not found during apply — skipping", model_id)
            continue

        signal_objective_sync = bool(row.get("signal.objective.sync.needed"))
        path_needs_update = row["model.path.current"] != row["model.path.target"]
        model_objective_sync = row["model.objective.current"] != row["model.objective.target"]
        model_store_needed = model_objective_sync or path_needs_update

        if row["s3.copy.needed"]:
            if s3 is None:
                logger.warning("S3 copy required but S3 is unavailable — skipping %s", signal_id)
                continue
            source_prefix = row["model.path.source.copy"]
            bucket = row["s3.bucket"]
            if not source_prefix or not bucket:
                logger.warning("Missing S3 copy source for signal %s — skipping", signal_id)
                continue
            if args.apply:
                copy_conversion_artifacts(
                    s3,
                    bucket,
                    source_prefix,
                    target_path,
                    logger,
                )
            else:
                logger.info(
                    "Would copy S3 artifacts from %s to %s in %s",
                    source_prefix,
                    target_path,
                    bucket,
                )

        new_model_id = model_id
        if model_store_needed:
            payload = build_model_store_payload(
                model,
                objective_id,
                signal,
                target_path=target_path if path_needs_update else None,
            )
            logger.info(
                "Storing model for %s | signal %s | model %s | objective=%s | path=%s",
                workspace_name,
                signal_id,
                model_id,
                objective_id,
                payload.get("path"),
            )
            if not args.apply:
                logger.info("Model payload: %s", json.dumps(payload, default=str))
            else:
                new_model_id = store_model(api_url, account_id, payload, logger)
                if not new_model_id:
                    logger.warning("models/store returned no id for signal %s", signal_id)
                    continue

        if not signal_objective_sync and not model_store_needed:
            continue

        if not args.apply:
            update_parts = []
            if signal_objective_sync:
                update_parts.append(f"objective={row['signal.objective.target']}")
            if model_store_needed:
                update_parts.append("model=<new-model-id>")
            logger.info(
                "Would call signals/update for %s with %s",
                signal_id,
                ", ".join(update_parts),
            )
            updated += 1
            continue

        update_signal(
            api_url,
            account_id,
            signal_id,
            logger,
            objective_id=row["signal.objective.target"] if signal_objective_sync else None,
            model_id=new_model_id if model_store_needed else None,
        )
        logger.info(
            "Signal %s updated: objective=%s | model %s -> %s | path=%s",
            signal_id,
            row["signal.objective.target"] if signal_objective_sync else row["signal.objective"],
            model_id,
            new_model_id if model_store_needed else model_id,
            target_path if path_needs_update else row["model.path.current"],
        )
        updated += 1

    if args.apply:
        logger.info("Updated %s signals.", updated)
    else:
        logger.info("Dry run only. Re-run with --apply to execute %s updates.", len(to_update))


if __name__ == "__main__":
    main()
