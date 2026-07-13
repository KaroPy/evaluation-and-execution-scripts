"""
Remove nested subdirectories under conversion model config folders in S3.

For each workspace bucket innkeepr-targeting-<sanitized_workspace_name>, scan
conversion model roots (<sanitized_workspace_name>-conversion-<goal|objective id>/)
and check whether configs/, configs_models_lstm/, or configs_models_likely/ contain
nested subdirectories. Config files are expected directly in those folders; nested
folders are leftovers from incorrect copies and are listed and deleted.

Dry-run is the default. Pass --apply to delete objects.

Usage (from repo root):
    python SprintStories/EN-xx-goals-to-objectives/cleanup_configs_subdirectories.py
    python SprintStories/EN-xx-goals-to-objectives/cleanup_configs_subdirectories.py --customer Rosental
    python SprintStories/EN-xx-goals-to-objectives/cleanup_configs_subdirectories.py --apply --customer Rosental
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.conncet_s3 import S3Connection  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402
from general_functions.sanitize_accout_name import sanitize_account_name  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_REPORT_OUTPUT = DATA_DIR / f"cleanup_configs_subdirectories_{datetime.now()}.csv"

CONFIG_DIRS = ("configs", "configs_models_lstm", "configs_models_likely")
DELETE_BATCH_SIZE = 1000


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"cleanup_configs_subdirectories_{stamp}.log"


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


def targeting_bucket(workspace_name: str) -> str:
    return f"innkeepr-targeting-{sanitize_account_name(workspace_name)}"


def conversion_root_prefix(workspace_name: str) -> str:
    return f"{sanitize_account_name(workspace_name)}-conversion-"


def bucket_exists(s3: S3Connection, bucket: str) -> bool:
    return bucket in s3.list_buckets()


def list_conversion_roots(s3: S3Connection, bucket: str, workspace_name: str) -> list[str]:
    root_prefix = conversion_root_prefix(workspace_name)
    entries = s3.list_files_with_pagination(bucket, root_prefix, delimiter="/")
    return sorted(
        {
            entry if entry.endswith("/") else f"{entry}/"
            for entry in entries
            if entry.startswith(root_prefix) and entry != root_prefix
        }
    )


def list_immediate_subdirectories(s3: S3Connection, bucket: str, prefix: str) -> list[str]:
    normalized = prefix if prefix.endswith("/") else f"{prefix}/"
    entries = s3.list_files_with_pagination(bucket, normalized, delimiter="/")
    return sorted(
        entry
        for entry in entries
        if entry != normalized and entry.startswith(normalized) and entry.endswith("/")
    )


def list_object_keys(s3: S3Connection, bucket: str, prefix: str) -> list[str]:
    paginator = s3.s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def delete_prefix(
    s3: S3Connection,
    bucket: str,
    prefix: str,
    apply: bool,
    logger: logging.Logger,
) -> int:
    keys = list_object_keys(s3, bucket, prefix)
    if not keys:
        return 0

    if not apply:
        for key in keys:
            logger.info("Would delete s3://%s/%s", bucket, key)
        return len(keys)

    deleted = 0
    for index in range(0, len(keys), DELETE_BATCH_SIZE):
        batch = keys[index : index + DELETE_BATCH_SIZE]
        s3.s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        deleted += len(batch)
        for key in batch:
            logger.info("Deleted s3://%s/%s", bucket, key)
    return deleted


def scan_workspace(
    s3: S3Connection,
    workspace_name: str,
    apply: bool,
    logger: logging.Logger,
) -> list[dict]:
    bucket = targeting_bucket(workspace_name)
    rows: list[dict] = []

    if not bucket_exists(s3, bucket):
        logger.info("Bucket %s does not exist — skipping %s", bucket, workspace_name)
        return rows

    conversion_roots = list_conversion_roots(s3, bucket, workspace_name)
    logger.info(
        "Workspace %s: bucket=%s, conversion roots=%s",
        workspace_name,
        bucket,
        len(conversion_roots),
    )

    for conversion_root in conversion_roots:
        for config_dir in CONFIG_DIRS:
            configs_prefix = f"{conversion_root.rstrip('/')}/{config_dir}/"
            subdirectories = list_immediate_subdirectories(s3, bucket, configs_prefix)
            if not subdirectories:
                continue

            for subdirectory in subdirectories:
                object_keys = list_object_keys(s3, bucket, subdirectory)
                row = {
                    "workspace.name": workspace_name,
                    "s3.bucket": bucket,
                    "conversion.root": conversion_root.rstrip("/"),
                    "configs.prefix": configs_prefix.rstrip("/"),
                    "subdirectory": subdirectory.rstrip("/"),
                    "object.count": len(object_keys),
                    "action": "delete" if object_keys else "skip",
                    "reason": "nested configs subdirectory",
                }
                rows.append(row)

                logger.info(
                    "%s | %s | %s objects under %s",
                    workspace_name,
                    configs_prefix,
                    len(object_keys),
                    subdirectory,
                )
                if object_keys:
                    delete_prefix(s3, bucket, subdirectory, apply, logger)

    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "List and delete nested subdirectories under conversion model config "
            "folders in innkeepr-targeting buckets."
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_false",
        dest="apply",
        help="Preview subdirectories and objects to delete (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        dest="apply",
        help="Delete nested config subdirectories and their objects",
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
        "--report-output",
        type=Path,
        default=DEFAULT_REPORT_OUTPUT,
        help=f"CSV report output path (default: {DEFAULT_REPORT_OUTPUT.name})",
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

    s3 = S3Connection()
    all_rows: list[dict] = []
    for workspace in workspaces:
        logger.info("=== Workspace: %s (%s) ===", workspace["name"], workspace["id"])
        all_rows.extend(scan_workspace(s3, workspace["name"], args.apply, logger))

    report = pd.DataFrame(all_rows)
    args.report_output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(args.report_output, index=False)
    logger.info("Saved report to %s", args.report_output)

    if report.empty:
        logger.info("No nested config subdirectories found.")
        return

    to_delete = report[report["action"] == "delete"]
    total_objects = int(to_delete["object.count"].sum())
    logger.info(
        "Found %s nested subdirectories with %s objects across %s workspaces",
        len(to_delete),
        total_objects,
        report["workspace.name"].nunique(),
    )

    if args.apply:
        logger.info("Deleted %s objects.", total_objects)
    else:
        logger.info(
            "Dry run only. Re-run with --apply to delete %s objects.",
            total_objects,
        )


if __name__ == "__main__":
    main()
