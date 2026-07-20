"""
Compare model.created date with the date embedded in model.path.

For each workspace, query all models and check whether the created timestamp
falls on the same calendar day as the path folder date, e.g.:
  more-conversion-<id>/2025-12-15_best_models/
  more-aud-<id>/2025-12-15_best_models_lstm/

Optionally fix models where need_fix is True by restoring model.created from path.created.

Usage (from repo root):
    python SprintStories/EN-xx-goals-to-objectives/check_model_created_vs_path_date.py
    python SprintStories/EN-xx-goals-to-objectives/check_model_created_vs_path_date.py --customer More
    python SprintStories/EN-xx-goals-to-objectives/check_model_created_vs_path_date.py --fix
    python SprintStories/EN-xx-goals-to-objectives/check_model_created_vs_path_date.py --fix --apply
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from goals_to_objectives import get_model, query_all, store_model  # noqa: E402

from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402

DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
PATH_DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")
MODIFICATION_DATES = frozenset({"2026-07-14", "2026-07-15"})


def default_output_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / f"model_created_vs_path_date_{stamp}.csv"


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"check_model_created_vs_path_date_{stamp}.log"


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


def date_from_model_path(path: str | None) -> str | None:
    if not path:
        return None

    normalized = path.rstrip("/")
    folder = normalized.rsplit("/", 1)[-1] if "/" in normalized else normalized
    match = PATH_DATE_PATTERN.search(folder)
    if match:
        return match.group(1)

    match = PATH_DATE_PATTERN.search(normalized)
    return match.group(1) if match else None


def created_date(created: str | None) -> str | None:
    if not created:
        return None
    return created[:10]


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def created_diff_days(model_created: str | None, path_created: str | None) -> int | None:
    model_day = parse_date(created_date(model_created))
    path_day = parse_date(path_created)
    if model_day is None or path_day is None:
        return None
    return (model_day - path_day).days


def is_modification_date(model_created: str | None) -> bool:
    return created_date(model_created) in MODIFICATION_DATES


def created_timestamp_from_path(path_created: str) -> str:
    return f"{path_created}T05:00:00.000Z"


def build_created_fix_payload(model: dict, path_created: str) -> dict:
    payload = model.copy()
    model_id = model.get("id")
    if not model_id:
        raise ValueError("model.id is required to update an existing model via models/store")
    payload["created"] = created_timestamp_from_path(path_created)
    return payload


def apply_created_fixes(
    api_url: str,
    fix_rows: pd.DataFrame,
    workspace_ids: dict[str, str],
    logger: logging.Logger,
    *,
    apply: bool,
) -> tuple[int, int]:
    updated = 0
    skipped = 0

    for _, row in fix_rows.iterrows():
        workspace_name = row["workspace.name"]
        model_id = row["model.id"]
        path_created = row["path.created"]
        account_id = workspace_ids.get(workspace_name)
        logger.info("###### Workspace: %s (%s) ######", workspace_name, model_id)
        if not account_id or not path_created:
            logger.warning("Skipping %s — missing workspace id or path.created", model_id)
            skipped += 1
            continue

        model = get_model(api_url, account_id, str(model_id), logger)
        if model is None:
            logger.warning("Model %s not found — skipping", model_id)
            skipped += 1
            continue

        old_payload = model.copy()
        new_payload = build_created_fix_payload(model, str(path_created))
        logger.info(
            "%s model %s (%s) | created %s -> %s",
            "Updating" if apply else "Would update",
            model_id,
            workspace_name,
            old_payload.get("created"),
            new_payload.get("created"),
        )
        if new_payload.get("scope") is None:
            new_payload.pop("scope")
        logger.info("Old payload: %s", json.dumps(old_payload, default=str))
        logger.info("New payload: %s", json.dumps(new_payload, default=str))

        if apply:
            stored_id = store_model(api_url, account_id, new_payload, logger)
            if not stored_id:
                logger.warning("models/store failed for model %s", model_id)
                skipped += 1
                continue
            updated += 1
        else:
            updated += 1

    return updated, skipped


def audit_workspace_models(
    workspace_name: str,
    account_id: str,
    models: list[dict],
) -> list[dict]:
    rows: list[dict] = []
    for model in models:
        model_created = model.get("created")
        path_created = date_from_model_path(model.get("path"))
        created_day = created_date(model_created)
        diff_days = created_diff_days(model_created, path_created)
        modification_date = is_modification_date(model_created)
        equal = path_created is not None and created_day is not None and created_day == path_created
        rows.append(
            {
                "workspace.name": workspace_name,
                "model.id": model.get("id"),
                "model.created": model_created,
                "path.created": path_created,
                "created.diff_days": diff_days,
                "modification_date": modification_date,
                "equal": equal,
                "need_fix": modification_date and not equal,
            }
        )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check whether model.created matches the date in model.path."
    )
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --customer More",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="CSV output path (default: data/model_created_vs_path_date_<timestamp>.csv)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file path",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Fix models where need_fix is True by setting model.created from path.created",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply fixes via models/store (default with --fix is dry-run only)",
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

    all_rows: list[dict] = []
    for workspace in workspaces:
        workspace_name = workspace["name"]
        account_id = workspace["id"]
        logger.info("=== Workspace: %s (%s) ===", workspace_name, account_id)

        models = query_all(f"{api_url}/api/models/query", account_id, {}, logger)
        rows = audit_workspace_models(workspace_name, account_id, models)
        all_rows.extend(rows)

        with_path_date = sum(1 for row in rows if row["path.created"] is not None)
        equal_count = sum(1 for row in rows if row["equal"])
        logger.info(
            "Workspace %s: %s models | with path date=%s | equal=%s | unequal=%s",
            workspace_name,
            len(rows),
            with_path_date,
            equal_count,
            with_path_date - equal_count,
        )

    df = pd.DataFrame(
        all_rows,
        columns=[
            "workspace.name",
            "model.id",
            "model.created",
            "path.created",
            "created.diff_days",
            "modification_date",
            "equal",
            "need_fix",
        ],
    )

    output_path = args.output or default_output_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Wrote %s rows to %s", len(df), output_path)

    comparable = df["path.created"].notna()
    logger.info(
        "Summary: %s models across %s workspaces | comparable=%s | equal=%s | unequal=%s | need_fix=%s",
        len(df),
        len(workspaces),
        comparable.sum(),
        df.loc[comparable, "equal"].sum(),
        (~df.loc[comparable, "equal"]).sum(),
        df["need_fix"].sum(),
    )

    if args.fix:
        fix_rows = df[df["need_fix"]].copy()
        workspace_ids = {workspace["name"]: workspace["id"] for workspace in workspaces}
        if fix_rows.empty:
            logger.info("No models with need_fix — nothing to fix.")
            return

        logger.info(
            "%s %s models with need_fix",
            "Applying fixes to" if args.apply else "Dry run for",
            len(fix_rows),
        )
        updated, skipped = apply_created_fixes(
            api_url,
            fix_rows,
            workspace_ids,
            logger,
            apply=args.apply,
        )
        if args.apply:
            logger.info("Updated %s models (%s skipped).", updated, skipped)
        else:
            logger.info("Dry run only. Re-run with --fix --apply to store %s models.", updated)


if __name__ == "__main__":
    main()
