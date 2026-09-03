"""
Compare consecutive model parameter settings for a signal.

For a workspace + signal.id:
  1. Load the signal
  2. Query models via models/query {"audienceId": signal.id}
  3. Sort models by created (oldest -> newest)
  4. Compare each previous model with the next (afterwards)
  5. Write a JSON report with before/after for every changed field,
     including timestamps of both models

Usage (from repo root):
    python scripts/interventions/check_signal_model_parameters.py \\
        --customer Tchibo --signal-id 68b69dff2e5f2d9a58cce692
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.call_api_with_account_id import (  # ruff: ignore[module-import-not-at-top-of-file]
    make_http_post_call,
    validate_response,
)
from general_functions.constants import (
    return_api_url,
)
from general_functions.return_workspace_ids import (
    return_workspace_ids,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"

CHECKABLE_PARAMETERS = (
    "audienceSize",
    "audienceSizePercentage",
    "conversionLag",
    "targetingOutlookDays",
    "trainingOutlookDays",
    "scope",
    "objective",
    "goal",
    "treatment",
    "type",
)


def default_output_path(workspace_name: str, signal_id: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_workspace = workspace_name.replace(" ", "_")
    return DATA_DIR / f"model_parameter_check_{safe_workspace}_{signal_id}_{stamp}.json"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"check_signal_model_parameters_{stamp}.log"


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


def query_all(
    endpoint_url: str,
    account_id: str,
    content: dict,
    logger: logging.Logger,
) -> list[dict]:
    logger.info("Querying %s content=%s", endpoint_url, content)
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


def resolve_workspace(customer: str, logger: logging.Logger) -> dict:
    workspaces = return_workspace_ids(tracking_started=False)
    matches = [ws for ws in workspaces if ws["name"] == customer]
    if not matches:
        raise SystemExit(f"Workspace '{customer}' not found.")
    if len(matches) > 1:
        logger.warning(
            "Multiple workspaces named %s; using the first (%s)",
            customer,
            matches[0]["id"],
        )
    return matches[0]


def values_equal(left: object, right: object) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)
    return left == right


def extract_parameters(model: dict) -> dict:
    return {key: deepcopy(model.get(key)) for key in CHECKABLE_PARAMETERS}


def normalize_treatment_ids(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return sorted({str(item) for item in value if item is not None and str(item)})
    text = str(value).strip()
    return [text] if text else []


def diff_treatments(before: object, after: object) -> dict[str, list[str]] | None:
    before_ids = set(normalize_treatment_ids(before))
    after_ids = set(normalize_treatment_ids(after))
    if before_ids == after_ids:
        return None

    added = sorted(after_ids - before_ids)
    removed = sorted(before_ids - after_ids)
    change: dict[str, list[str]] = {}
    if added:
        label = "add first time treatment" if not before_ids else "added treatment"
        change[label] = added
    if removed:
        change["removed treatment"] = removed
    return change or None


def diff_parameters(previous: dict, afterwards: dict) -> dict[str, dict]:
    changes: dict[str, dict] = {}
    for key in CHECKABLE_PARAMETERS:
        before = previous.get(key)
        after = afterwards.get(key)
        if key == "treatment":
            treatment_change = diff_treatments(before, after)
            if treatment_change is not None:
                changes[key] = treatment_change
            continue
        if values_equal(before, after):
            continue
        changes[key] = {"before": before, "after": after}
    return changes


def compare_consecutive_models(
    models: list[dict],
    linked_model_id: str | None,
    logger: logging.Logger,
) -> list[dict]:
    ordered = sorted(models, key=lambda row: row.get("created") or "")
    results: list[dict] = []

    for previous, afterwards in zip(ordered, ordered[1:]):
        changes = diff_parameters(
            extract_parameters(previous),
            extract_parameters(afterwards),
        )
        if not changes:
            continue

        entry = {
            "previous.model.id": previous.get("id"),
            "previous.model.created": previous.get("created"),
            "previous.model.path": previous.get("path"),
            "previous.is_linked_model": previous.get("id") == linked_model_id,
            "new.model.id": afterwards.get("id"),
            "new.model.created": afterwards.get("created"),
            "new.model.path": afterwards.get("path"),
            "new.is_linked_model": afterwards.get("id") == linked_model_id,
            "changes": changes,
        }
        results.append(entry)
        logger.info(
            "Change %s (%s) -> %s (%s): %s",
            previous.get("id"),
            previous.get("created"),
            afterwards.get("id"),
            afterwards.get("created"),
            json.dumps(changes, default=str, sort_keys=True),
        )

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare consecutive models for a signal (sorted by created) "
            "and write before/after parameter diffs to JSON."
        )
    )
    parser.add_argument(
        "--customer",
        required=True,
        metavar="NAME",
        help="Workspace name, e.g. --customer Tchibo",
    )
    parser.add_argument(
        "--signal-id",
        required=True,
        dest="signal_id",
        metavar="ID",
        help="Signal / audience id",
    )
    parser.add_argument(
        "--output",
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
    workspace = resolve_workspace(args.customer, logger)
    account_id = workspace["id"]

    signals = query_all(
        f"{api_url}/api/signals/query",
        account_id,
        {"id": args.signal_id},
        logger,
    )
    if not signals:
        raise SystemExit(f"Signal '{args.signal_id}' not found in workspace '{workspace['name']}'.")
    signal = signals[0]
    linked_model_id = signal.get("model")

    models = query_all(
        f"{api_url}/api/models/query",
        account_id,
        {"audienceId": args.signal_id},
        logger,
    )
    models = [model for model in models if model.get("audience") == args.signal_id]

    if not models and linked_model_id:
        models = query_all(
            f"{api_url}/api/models/query",
            account_id,
            {"id": linked_model_id},
            logger,
        )

    transitions = compare_consecutive_models(models, linked_model_id, logger)

    report = {
        "workspace.name": workspace["name"],
        "workspace.id": account_id,
        "signal.id": args.signal_id,
        "signal.name": signal.get("name"),
        "signal.model": linked_model_id,
        "checked.parameters": list(CHECKABLE_PARAMETERS),
        "models.queried": len(models),
        "transitions.compared": max(len(models) - 1, 0),
        "transitions.with_changes": len(transitions),
        "transitions": transitions,
    }

    output_path = args.output or default_output_path(workspace["name"], args.signal_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=str)

    logger.info(
        "Wrote report: transitions_with_changes=%s / compared=%s -> %s",
        len(transitions),
        max(len(models) - 1, 0),
        output_path.resolve(),
    )


if __name__ == "__main__":
    main()
