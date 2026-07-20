"""
Audit model.objective across workspaces.

For each workspace:
  1. Query all models (models/query)
  2. Split models with and without objective
  3. For paths ending in _best_models/, suggest borrowing the objective from another
     model whose conversion root matches on goal.id or objective id
  4. For causal models on <customer>-aud-<id>/ paths, use the workspace objective
     that matches the model goal (goal.conversionEvents == objective.events)
  5. Apply models_goal_to_objective_handling.json rules for configured non-standard paths
     (optional model.audience; path.suffixes as exact map or suffix list; action
     use_matched_objective_to_goal_id, or fixed objective_id, optionally scoped by goal_id)
  6. Write remaining models with other path endings to JSON for manual review
  7. Ignore models whose path contains -exp-

Dry-run is the default. Pass --apply to update existing models via models/store.

Usage (from repo root):
    python SprintStories/EN-xx-goals-to-objectives/audit_model_objectives.py
    python SprintStories/EN-xx-goals-to-objectives/audit_model_objectives.py --customer Rosental
    python SprintStories/EN-xx-goals-to-objectives/audit_model_objectives.py --apply --customer Rosental
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from goals_to_objectives import (  # noqa: E402
    build_model_store_payload,
    get_goal,
    objective_matches_goal_events,
    report_remaining_models_without_objective,
    store_model,
    targeting_bucket,
)

from general_functions.call_api_with_account_id import (  # noqa: E402
    make_http_post_call,
    validate_response,
)
from general_functions.conncet_s3 import S3Connection  # noqa: E402
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402

DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_HANDLING_CONFIG = SCRIPT_DIR / "models_goal_to_objective_handling.json"

BACKFILL_PATH_SUFFIX = "_best_models/"
EXP_PATH_MARKER = "-exp-"


def is_exp_path(path: str | None) -> bool:
    return EXP_PATH_MARKER in (path or "")


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"audit_model_objectives_{stamp}.log"


def default_json_output() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"model_objective_audit_{stamp}.json"


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


def query_all(endpoint_url: str, account_id: str, content: dict, logger: logging.Logger) -> list:
    logger.info("Querying %s", endpoint_url)
    next_page = 1
    data: list = []
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
        pagination = json_body.get("pagination") or {}
        next_page = pagination.get("next")
    logger.info("Fetched %s elements", len(data))
    return data


def normalize_model_path(path: str | None) -> str:
    if not path:
        return ""
    return path if path.endswith("/") else f"{path.rstrip('/')}/"


def path_folder_suffix(path: str | None) -> str:
    normalized = normalize_model_path(path)
    if not normalized or "/" not in normalized.rstrip("/"):
        return normalized.rstrip("/")
    return normalized.rstrip("/").rsplit("/", 1)[-1] + "/"


def is_backfill_path(path: str | None) -> bool:
    return normalize_model_path(path).endswith(BACKFILL_PATH_SUFFIX)


def conversion_prefix_from_path(path: str | None) -> str | None:
    root = normalize_model_path(path).split("/")[0]
    if "-conversion-" not in root:
        return None
    return root.rsplit("-conversion-", 1)[0] + "-conversion-"


def conversion_keys_for_model(model: dict) -> set[str]:
    path = normalize_model_path(model.get("path"))
    prefix = conversion_prefix_from_path(path)
    if not prefix:
        return set()

    keys: set[str] = {path.split("/")[0]}
    goal_id = model.get("goal")
    objective_id = model.get("objective")
    if goal_id:
        keys.add(f"{prefix}{goal_id}")
    if objective_id:
        keys.add(f"{prefix}{objective_id}")
    return keys


def objective_for_conversion_group(
    model: dict,
    models_with_objective: list[dict],
) -> tuple[str | None, list[str], list[str]]:
    keys = conversion_keys_for_model(model)
    if not keys:
        return None, [], []

    related_by_id: dict[str, dict] = {}
    matched_keys: set[str] = set()
    for other in models_with_objective:
        if other.get("id") == model.get("id"):
            continue
        shared = keys & conversion_keys_for_model(other)
        if shared:
            related_by_id[str(other["id"])] = other
            matched_keys.update(shared)

    if not related_by_id:
        return None, sorted(matched_keys), []

    objectives: dict[str, list[str]] = defaultdict(list)
    for other in related_by_id.values():
        objective_id = other.get("objective")
        model_id = other.get("id")
        if objective_id and model_id:
            objectives[str(objective_id)].append(str(model_id))

    if not objectives:
        return None, sorted(matched_keys), []

    if len(objectives) == 1:
        return next(iter(objectives)), sorted(matched_keys), []

    conflicts = [
        f"objective={objective_id} model_ids={model_ids}"
        for objective_id, model_ids in objectives.items()
    ]
    preferred = max(objectives.items(), key=lambda item: len(item[1]))[0]
    return preferred, sorted(matched_keys), conflicts


def model_row(model: dict, workspace_name: str, account_id: str) -> dict:
    path = model.get("path")
    conversion_keys = sorted(conversion_keys_for_model(model))
    return {
        "workspace.name": workspace_name,
        "workspace.id": account_id,
        "model.id": model.get("id"),
        "model.type": model.get("type"),
        "model.goal": model.get("goal"),
        "model.audience": model.get("audience"),
        "model.targetingOutlookDays": model.get("targetingOutlookDays"),
        "model.path": path,
        "model.path.suffix": path_folder_suffix(path),
        "model.conversion.keys": conversion_keys,
        "model.objective": model.get("objective"),
        "model.created": model.get("created"),
        "model.f1Score": model.get("f1Score"),
    }


def is_aud_path(path: str | None) -> bool:
    root = normalize_model_path(path).split("/")[0]
    return "-aud-" in root


def aud_root_from_path(path: str | None) -> str | None:
    root = normalize_model_path(path).split("/")[0]
    return root if is_aud_path(path) else None


def objective_for_goal(
    goal: dict | None,
    objectives: list[dict],
    logger: logging.Logger,
) -> tuple[str | None, list[str]]:
    if not goal:
        return None, ["goal not found"]

    matches = [
        objective for objective in objectives if objective_matches_goal_events(objective, goal)
    ]
    if len(matches) == 1:
        return str(matches[0]["id"]), []
    if len(matches) > 1:
        logger.warning("Multiple objectives match goal %s events", goal.get("id"))
        return None, [f"multiple objectives match goal {goal.get('id')}"]
    return None, [f"no objective matches goal {goal.get('id')} events"]


def model_eligible_for_backfill(row: dict) -> tuple[bool, str | None]:
    missing: list[str] = []
    if not row.get("model.audience"):
        missing.append("model.audience")
    if row.get("model.targetingOutlookDays") is None:
        missing.append("model.targetingOutlookDays")
    if not missing:
        return True, None
    return False, f"missing {', '.join(missing)}"


def append_backfill_candidate(
    *,
    row: dict,
    suggested_objective: str | None,
    match_type: str,
    matched_keys: list[str],
    conflicts: list[str],
    reason_ok: str,
    reason_missing: str,
    backfill_candidates: list[dict],
    missing_candidates: list[dict],
) -> None:
    candidate = {
        **row,
        "suggested.objective": suggested_objective,
        "backfill.match.type": match_type,
        "matched.conversion.keys": matched_keys,
        "conversion.objective.conflicts": conflicts,
        "action": "update" if suggested_objective else "review",
        "reason": reason_ok if suggested_objective else reason_missing,
    }
    if suggested_objective:
        eligible, eligibility_reason = model_eligible_for_backfill(candidate)
        if eligible:
            backfill_candidates.append(candidate)
        else:
            candidate["action"] = "review"
            candidate["reason"] = f"excluded from backfill: {eligibility_reason}"
            missing_candidates.append(candidate)
    else:
        missing_candidates.append(candidate)


def load_handling_config(config_path: Path, logger: logging.Logger) -> dict[str, list[dict]]:
    if not config_path.is_file():
        logger.info("No handling config at %s", config_path)
        return {}
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError(f"Handling config must be a JSON object: {config_path}")
    return config


def path_matches_handling_suffixes(
    rule_suffixes: dict | list | None,
    path_suffix: str,
    model_path: str | None,
) -> bool:
    if not rule_suffixes:
        return True

    normalized_suffix = path_suffix or ""
    normalized_path = normalize_model_path(model_path)
    if isinstance(rule_suffixes, dict):
        return normalized_suffix in rule_suffixes
    if isinstance(rule_suffixes, list):
        return any(
            normalized_suffix == pattern
            or normalized_suffix.endswith(pattern)
            or (pattern and normalized_path.endswith(pattern))
            for pattern in rule_suffixes
        )
    raise ValueError(f"path.suffixes must be a list or object, got {type(rule_suffixes).__name__}")


def matches_handling_rule(
    rule: dict,
    workspace_name: str,
    audience: str | None,
    path_suffix: str,
    model_path: str | None = None,
    goal_id: str | None = None,
) -> bool:
    rule_workspace = rule.get("workspace.name")
    if rule_workspace and rule_workspace != workspace_name:
        return False
    if "model.audience" in rule and rule.get("model.audience") != audience:
        return False
    rule_goal_id = rule.get("goal_id")
    if rule_goal_id is not None and str(rule_goal_id) != str(goal_id or ""):
        return False
    return path_matches_handling_suffixes(rule.get("path.suffixes"), path_suffix, model_path)


def find_handling_rule(
    workspace_name: str,
    audience: str | None,
    path_suffix: str,
    handling_config: dict[str, list[dict]],
    model_path: str | None = None,
    goal_id: str | None = None,
) -> dict | None:
    for rule in handling_config.get(workspace_name, []):
        if matches_handling_rule(
            rule,
            workspace_name,
            audience,
            path_suffix,
            model_path,
            goal_id,
        ):
            return rule
    return None


def resolve_handling_rule(
    rule: dict,
    model: dict,
    objectives: list[dict],
    objectives_by_id: dict[str, dict],
    api_url: str,
    account_id: str,
    goal_cache: dict[str, dict | None],
    logger: logging.Logger,
) -> tuple[str | None, list[str], str, str, str]:
    configured_objective_id = rule.get("objective_id")
    if configured_objective_id:
        objective_id = str(configured_objective_id)
        rule_goal_id = rule.get("goal_id")
        if objective_id not in objectives_by_id:
            return (
                None,
                [f"objective_id {objective_id} not found in workspace objectives"],
                "config_objective_id",
                f"use configured objective_id {objective_id}",
                f"configured objective_id {objective_id} not found in workspace objectives",
            )
        if rule_goal_id is not None:
            match_type = "config_goal_objective_id"
            reason_ok = (
                f"use configured objective_id {objective_id} "
                f"for goal_id {rule_goal_id} (config path match)"
            )
            reason_missing = (
                f"configured objective_id {objective_id} not found for goal_id {rule_goal_id}"
            )
        else:
            match_type = "config_objective_id"
            reason_ok = f"use configured objective_id {objective_id}"
            reason_missing = (
                f"configured objective_id {objective_id} not found in workspace objectives"
            )
        return (objective_id, [], match_type, reason_ok, reason_missing)

    action = rule.get("action")
    if action == "use_matched_objective_to_goal_id":
        goal = get_goal(api_url, account_id, model.get("goal"), logger, goal_cache)
        objective_id, issues = objective_for_goal(goal, objectives, logger)
        return (
            objective_id,
            issues,
            "config_goal_events",
            "use workspace objective matching model goal (config: use_matched_objective_to_goal_id)",
            "no workspace objective matches model goal (config: use_matched_objective_to_goal_id)",
        )
    raise ValueError(f"Unknown handling rule: {rule}")


def try_config_backfill(
    *,
    model: dict,
    row: dict,
    workspace_name: str,
    path_suffix: str,
    handling_config: dict[str, list[dict]],
    objectives: list[dict],
    api_url: str,
    account_id: str,
    goal_cache: dict[str, dict | None],
    logger: logging.Logger,
    backfill_candidates: list[dict],
    missing_candidates: list[dict],
) -> bool:
    rule = find_handling_rule(
        workspace_name,
        model.get("audience"),
        path_suffix,
        handling_config,
        model.get("path"),
        model.get("goal"),
    )
    if rule is None:
        return False

    if not rule.get("action") and not rule.get("objective_id"):
        return False

    objectives_by_id = {
        str(objective["id"]): objective for objective in objectives if objective.get("id")
    }
    suggested_objective, issues, match_type, reason_ok, reason_missing = resolve_handling_rule(
        rule,
        model,
        objectives,
        objectives_by_id,
        api_url,
        account_id,
        goal_cache,
        logger,
    )
    append_backfill_candidate(
        row={
            **row,
            "config.action": rule.get("action"),
            "config.goal_id": rule.get("goal_id"),
            "config.objective_id": rule.get("objective_id"),
            "config.path.suffixes": rule.get("path.suffixes"),
        },
        suggested_objective=suggested_objective,
        match_type=match_type,
        matched_keys=[
            key
            for key in [
                f"goal={model.get('goal')}" if model.get("goal") else None,
                f"config.goal_id={rule.get('goal_id')}" if rule.get("goal_id") else None,
                f"config.objective_id={rule.get('objective_id')}"
                if rule.get("objective_id")
                else None,
            ]
            if key
        ],
        conflicts=issues,
        reason_ok=reason_ok,
        reason_missing=reason_missing,
        backfill_candidates=backfill_candidates,
        missing_candidates=missing_candidates,
    )
    return True


def audit_workspace_models(
    workspace_name: str,
    account_id: str,
    models: list[dict],
    objectives: list[dict],
    api_url: str,
    logger: logging.Logger,
    handling_config: dict[str, list[dict]],
) -> dict:
    audited_models = [model for model in models if not is_exp_path(model.get("path"))]
    ignored_exp_count = len(models) - len(audited_models)
    if ignored_exp_count:
        logger.info(
            "Workspace %s: ignoring %s models with -exp- paths", workspace_name, ignored_exp_count
        )

    models_with_objective = [model for model in audited_models if model.get("objective")]
    goal_cache: dict[str, dict | None] = {}

    with_objective: list[dict] = []
    without_objective: list[dict] = []
    backfill_candidates: list[dict] = []
    missing_backfill_objective: list[dict] = []
    non_standard_path_models: list[dict] = []

    for model in audited_models:
        row = model_row(model, workspace_name, account_id)
        if is_aud_path(model.get("path")):
            row["model.aud.root"] = aud_root_from_path(model.get("path"))

        if model.get("objective"):
            with_objective.append(row)
        else:
            without_objective.append(row)

        if model.get("objective"):
            if not is_backfill_path(model.get("path")) and not is_aud_path(model.get("path")):
                path_suffix = path_folder_suffix(model.get("path"))
                if not find_handling_rule(
                    workspace_name,
                    model.get("audience"),
                    path_suffix,
                    handling_config,
                    model.get("path"),
                    model.get("goal"),
                ):
                    non_standard_path_models.append(row)
            continue

        path = model.get("path")
        if is_aud_path(path) and model.get("type") == "causal":
            path_suffix = path_folder_suffix(path)
            if try_config_backfill(
                model=model,
                row=row,
                workspace_name=workspace_name,
                path_suffix=path_suffix,
                handling_config=handling_config,
                objectives=objectives,
                api_url=api_url,
                account_id=account_id,
                goal_cache=goal_cache,
                logger=logger,
                backfill_candidates=backfill_candidates,
                missing_candidates=missing_backfill_objective,
            ):
                continue

            goal = get_goal(api_url, account_id, model.get("goal"), logger, goal_cache)
            suggested_objective, issues = objective_for_goal(goal, objectives, logger)
            append_backfill_candidate(
                row=row,
                suggested_objective=suggested_objective,
                match_type="goal_events",
                matched_keys=[f"goal={model.get('goal')}"] if model.get("goal") else [],
                conflicts=issues,
                reason_ok="use workspace objective matching model goal events",
                reason_missing="no workspace objective matches model goal events",
                backfill_candidates=backfill_candidates,
                missing_candidates=missing_backfill_objective,
            )
            continue

        if is_backfill_path(path):
            path_suffix = path_folder_suffix(path)
            if try_config_backfill(
                model=model,
                row=row,
                workspace_name=workspace_name,
                path_suffix=path_suffix,
                handling_config=handling_config,
                objectives=objectives,
                api_url=api_url,
                account_id=account_id,
                goal_cache=goal_cache,
                logger=logger,
                backfill_candidates=backfill_candidates,
                missing_candidates=missing_backfill_objective,
            ):
                continue

            sibling_objective, matched_keys, conflicts = objective_for_conversion_group(
                model,
                models_with_objective,
            )
            append_backfill_candidate(
                row=row,
                suggested_objective=sibling_objective,
                match_type="conversion_root",
                matched_keys=matched_keys,
                conflicts=conflicts,
                reason_ok=(
                    "borrow objective from model with matching conversion root "
                    "(goal.id or objective id)"
                ),
                reason_missing="no objective found for matching conversion root",
                backfill_candidates=backfill_candidates,
                missing_candidates=missing_backfill_objective,
            )
            continue

        path_suffix = path_folder_suffix(path)
        if try_config_backfill(
            model=model,
            row=row,
            workspace_name=workspace_name,
            path_suffix=path_suffix,
            handling_config=handling_config,
            objectives=objectives,
            api_url=api_url,
            account_id=account_id,
            goal_cache=goal_cache,
            logger=logger,
            backfill_candidates=backfill_candidates,
            missing_candidates=missing_backfill_objective,
        ):
            continue

        non_standard_path_models.append(row)

    logger.info(
        "Workspace %s: %s models (%s with objective, %s without, %s backfill candidates, "
        "%s non-standard paths, %s ignored -exp- paths)",
        workspace_name,
        len(audited_models),
        len(with_objective),
        len(without_objective),
        len(backfill_candidates),
        len(non_standard_path_models),
        ignored_exp_count,
    )

    return {
        "workspace.name": workspace_name,
        "workspace.id": account_id,
        "model.count": len(audited_models),
        "ignored_exp_path.count": ignored_exp_count,
        "with_objective.count": len(with_objective),
        "without_objective.count": len(without_objective),
        "backfill_candidates.count": len(backfill_candidates),
        "missing_backfill_objective.count": len(missing_backfill_objective),
        "non_standard_path.count": len(non_standard_path_models),
        "backfill_candidates": backfill_candidates,
        "missing_backfill_objective": missing_backfill_objective,
        "non_standard_path_models": non_standard_path_models,
    }


WORKSPACE_SUMMARY_KEYS = (
    "workspace.name",
    "workspace.id",
    "model.count",
    "with_objective.count",
    "without_objective.count",
    "backfill_candidates.count",
    "missing_backfill_objective.count",
    "non_standard_path.count",
    "ignored_exp_path.count",
)


def summarize_non_standard_by_audience(rows: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for row in rows:
        audience_id = row.get("model.audience") or "(no audience)"
        entry = grouped.setdefault(
            audience_id,
            {
                "model.audience": audience_id,
                "workspace.name": row.get("workspace.name"),
                "model.count": 0,
                "with_objective.count": 0,
                "model.types": defaultdict(int),
                "path.suffixes": defaultdict(int),
            },
        )
        entry["model.count"] += 1
        if row.get("model.objective"):
            entry["with_objective.count"] += 1
        model_type = row.get("model.type") or "(none)"
        entry["model.types"][model_type] += 1
        suffix = row.get("model.path.suffix") or "(none)"
        entry["path.suffixes"][suffix] += 1

    summary: list[dict] = []
    for audience_id, entry in sorted(
        grouped.items(),
        key=lambda item: (-item[1]["model.count"], item[0]),
    ):
        summary.append(
            {
                "model.audience": audience_id,
                "workspace.name": entry["workspace.name"],
                "model.count": entry["model.count"],
                "with_objective.count": entry["with_objective.count"],
                "model.types": dict(sorted(entry["model.types"].items())),
                "path.suffixes": dict(
                    sorted(entry["path.suffixes"].items(), key=lambda item: (-item[1], item[0]))
                ),
            }
        )
    return summary


def summarize_reason_counts(rows: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row.get("reason") or "(no reason)"] += 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def summarize_missing_backfill_by_reason(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], int] = defaultdict(int)
    for row in rows:
        workspace = row.get("workspace.name") or "(unknown)"
        reason = row.get("reason") or "(no reason)"
        grouped[workspace, reason] += 1

    return [
        {
            "workspace.name": workspace,
            "reason": reason,
            "model.count": count,
        }
        for (workspace, reason), count in sorted(
            grouped.items(),
            key=lambda item: (-item[1], item[0][0], item[0][1]),
        )
    ]


def build_report(workspace_reports: list[dict]) -> dict:
    totals = {
        "workspaces": len(workspace_reports),
        "models": sum(report["model.count"] for report in workspace_reports),
        "with_objective": sum(report["with_objective.count"] for report in workspace_reports),
        "without_objective": sum(report["without_objective.count"] for report in workspace_reports),
        "backfill_candidates": sum(
            report["backfill_candidates.count"] for report in workspace_reports
        ),
        "missing_backfill_objective": sum(
            report["missing_backfill_objective.count"] for report in workspace_reports
        ),
        "non_standard_path_models": sum(
            report["non_standard_path.count"] for report in workspace_reports
        ),
        "ignored_exp_path_models": sum(
            report.get("ignored_exp_path.count", 0) for report in workspace_reports
        ),
    }
    non_standard_path_models = [
        row for report in workspace_reports for row in report["non_standard_path_models"]
    ]
    missing_backfill_objective = [
        row for report in workspace_reports for row in report["missing_backfill_objective"]
    ]
    totals["missing_backfill_objective_by_reason"] = summarize_reason_counts(
        missing_backfill_objective
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "backfill_path_suffix": BACKFILL_PATH_SUFFIX,
        "summary": totals,
        "workspaces": [
            {
                **{key: report[key] for key in WORKSPACE_SUMMARY_KEYS if key in report},
                "missing_backfill_objective.by_reason": summarize_reason_counts(
                    report["missing_backfill_objective"]
                ),
            }
            for report in workspace_reports
        ],
        "non_standard_paths_by_audience": summarize_non_standard_by_audience(
            non_standard_path_models
        ),
        "non_standard_path_models": non_standard_path_models,
        "backfill_candidates": [
            row for report in workspace_reports for row in report["backfill_candidates"]
        ],
        "missing_backfill_objective_by_reason": summarize_missing_backfill_by_reason(
            missing_backfill_objective
        ),
        "missing_backfill_objective": missing_backfill_objective,
    }


def apply_backfill_candidate(
    api_url: str,
    account_id: str,
    workspace_name: str,
    candidate: dict,
    model: dict,
    s3: S3Connection | None,
    logger: logging.Logger,
) -> bool:
    objective_id = candidate["suggested.objective"]
    model_id = candidate["model.id"]
    model_created = model.get("created")
    if model.get("objective") == objective_id:
        logger.info("Model %s already has objective=%s — skipping", model_id, objective_id)
        return True

    logging.info(f"apply_backfill_candidate: {model_id}")
    bucket = targeting_bucket(workspace_name, model.get("path"))
    try:
        payload = build_model_store_payload(
            model,
            objective_id,
            signal={},
            s3=s3,
            bucket=bucket,
            logger=logger,
            fallback_paths=[model.get("path")],
        )
    except Exception as e:
        logging.warning(f"backfill will be ignored due to payload creation error: {e}")
        return False
    payload["id"] = model_id
    payload["created"] = model_created
    if not payload.get("audience") and candidate.get("model.audience"):
        payload["audience"] = candidate["model.audience"]
    if not payload.get("audience") and not candidate.get("model.audience"):
        logger.warning("Audience not in payload and not in candidate — skipping")
        return False
    if not payload.get("targetingOutlookDays"):
        logger.warning("Targeting outlook days not in payload — skipping")
        return False

    logging.info(f"Payload models/store: {payload}")

    updated_model_id = store_model(api_url, account_id, payload, logger)

    if not updated_model_id:
        logger.warning("models/store returned no id for model %s", model_id)
        return False

    logger.info(
        "Updated model %s with objective=%s",
        model_id,
        objective_id,
    )
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit model.objective and suggest objectives from same-path models."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_false",
        dest="apply",
        help="Write audit JSON only (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        dest="apply",
        help="Update existing models via models/store",
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
        "--json-output",
        type=Path,
        default=None,
        help="Audit JSON output path (default: data/model_objective_audit_<timestamp>.json)",
    )
    parser.add_argument(
        "--handling-config",
        type=Path,
        default=DEFAULT_HANDLING_CONFIG,
        help="JSON config for non-standard path handling rules",
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

    handling_config = load_handling_config(args.handling_config, logger)
    if handling_config:
        logger.info(
            "Loaded handling rules for %s workspace(s) from %s",
            len(handling_config),
            args.handling_config,
        )

    s3 = S3Connection() if args.apply else None
    workspace_reports: list[dict] = []

    for workspace in workspaces:
        workspace_name = workspace["name"]
        account_id = workspace["id"]
        logger.info("=== Workspace: %s (%s) ===", workspace_name, account_id)

        models = query_all(f"{api_url}/api/models/query", account_id, {}, logger)
        objectives = query_all(f"{api_url}/api/objectives/query", account_id, {}, logger)
        report = audit_workspace_models(
            workspace_name,
            account_id,
            models,
            objectives,
            api_url,
            logger,
            handling_config,
        )
        workspace_reports.append(report)

        if args.apply:
            if report["backfill_candidates"]:
                models_by_id = {str(model["id"]): model for model in models if model.get("id")}

                for candidate in report["backfill_candidates"]:
                    model_id = str(candidate["model.id"])
                    model = models_by_id.get(model_id)
                    if model is None:
                        logger.warning("Model %s not found during apply — skipping", model_id)
                        continue
                    apply_backfill_candidate(
                        api_url,
                        account_id,
                        workspace_name,
                        candidate,
                        model,
                        s3,
                        logger,
                    )

            try:
                report["post_apply"] = report_remaining_models_without_objective(
                    api_url,
                    [workspace],
                    logger,
                )
            except Exception as exc:
                logger.exception(
                    "Post-apply check failed for workspace %s — continuing to write audit JSON",
                    workspace_name,
                )
                report["post_apply"] = {
                    "error": str(exc),
                    "summary": {"workspaces": 1, "remaining_without_objective": None},
                    "remaining_without_objective": [],
                }

    audit_report = build_report(workspace_reports)
    post_apply_reports = [
        report["post_apply"] for report in workspace_reports if report.get("post_apply") is not None
    ]
    if post_apply_reports:
        audit_report["post_apply"] = {
            "summary": {
                "workspaces": len(post_apply_reports),
                "remaining_without_objective": sum(
                    item["summary"]["remaining_without_objective"] for item in post_apply_reports
                ),
            },
            "remaining_without_objective": [
                row for item in post_apply_reports for row in item["remaining_without_objective"]
            ],
        }

    json_output = args.json_output or default_json_output()
    if not args.apply:
        json_output = json_output.with_suffix(".dry-run.json")
    json_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(audit_report, indent=2, default=str), encoding="utf-8")
    logger.info("Wrote audit report to %s", json_output)

    summary = audit_report["summary"]
    logger.info(
        "Summary: %s models across %s workspaces | with objective=%s | without=%s | "
        "backfill candidates=%s | missing backfill=%s | non-standard paths=%s",
        summary["models"],
        summary["workspaces"],
        summary["with_objective"],
        summary["without_objective"],
        summary["backfill_candidates"],
        summary["missing_backfill_objective"],
        summary["non_standard_path_models"],
    )
    if summary.get("missing_backfill_objective_by_reason"):
        logger.info("Missing backfill reasons:")
        for reason, count in summary["missing_backfill_objective_by_reason"].items():
            logger.info("  %sx %s", count, reason)

    if not args.apply:
        logger.info("Dry run only. Re-run with --apply to update existing models.")


if __name__ == "__main__":
    main()
