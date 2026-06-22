"""
Overview of other exclusions per workspace.

For each workspace with other exclusions, writes a table where each row is an
other exclusion (not Visitor or Purchaser) plus a column listing existing
visitor exclusions on the same source.

Columns:
workspace.name, audience.id, audience.name, audience.source,
audience.targetingOutlookDays, audience.audienceSizePercentage,
existing Visitor Exclusions (for this source)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from check_signal_configuration import (
    EXCLUDED_SOURCES,
    api_post,
    fetch_model,
    is_visitor_exclusion_name,
    query_all_pages,
)
from src.paths import SCRIPT_DIR, SIGNAL_CONFIGURATION_DATA_DIR

OUTPUT_JSON_PATH = SIGNAL_CONFIGURATION_DATA_DIR / "other_exclusions_overview.json"
OUTPUT_CSV_PATH = SIGNAL_CONFIGURATION_DATA_DIR / "other_exclusions_overview.csv"
OUTPUT_MD_PATH = SIGNAL_CONFIGURATION_DATA_DIR / "other_exclusions_overview.md"

VISITOR_EXCLUSIONS_COLUMN = "existing Visitor Exclusions (for this source)"
PURCHASER_EXCLUSIONS_COLUMN = "existing Purchaser Exclusions (for this source)"

OUTPUT_COLUMNS = [
    "workspace.name",
    "audience.id",
    "audience.name",
    "audience.source",
    "audience.targetingOutlookDays",
    "audience.audienceSizePercentage",
    VISITOR_EXCLUSIONS_COLUMN,
    PURCHASER_EXCLUSIONS_COLUMN,
]


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = SIGNAL_CONFIGURATION_DATA_DIR.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"overview_other_exclusions_{stamp}.log"


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


def is_exclusion_audience(audience_type: str | None, audience_name: str) -> bool:
    return audience_type == "exclusion" or "Exclusion" in audience_name


def is_purchaser_exclusion_name(audience_name: str) -> bool:
    return "Purchaser" in audience_name


def build_exclusion_record(
    audience: dict,
    connection_name: str,
    model: dict,
) -> dict:
    config = audience.get("config", {})
    return {
        "audience.id": audience["id"],
        "audience.name": audience["name"],
        "audience.source": connection_name,
        "audience.targetingOutlookDays": config.get("targetingOutlookDays"),
        "audience.audienceSizePercentage": model.get("audienceSizePercentage"),
    }


def format_exclusions_for_source(
    exclusions_by_source: dict[str, list[str]],
    source: str,
) -> str:
    names = exclusions_by_source.get(source) or []
    if not names:
        return ""
    return "; ".join(names)


def sort_exclusions_by_source(
    exclusions_by_source: dict[str, list[str]],
) -> None:
    for names in exclusions_by_source.values():
        names.sort()


def collect_workspace_exclusions(
    api_url: str,
    token: str,
    workspace_filter: set[str] | None = None,
) -> list[dict]:
    workspace_data: list[dict] = []
    workspaces = api_post(f"{api_url}/api/core/workspaces/query", token, {"content": {}})

    for workspace in sorted(workspaces, key=lambda item: item["name"]):
        workspace_name = workspace["name"]
        if workspace_filter and workspace_name not in workspace_filter:
            continue

        workspace_id = workspace["id"]
        connections = api_post(
            f"{api_url}/api/connections/query",
            token,
            {"content": {}, "context": {"workspaceId": workspace_id}},
        )
        connection_names = {
            conn.get("_id") or conn.get("id"): conn["name"] for conn in connections
        }

        other_exclusions: list[dict] = []
        visitor_names_by_source: dict[str, list[str]] = {}
        purchaser_names_by_source: dict[str, list[str]] = {}

        audiences = query_all_pages(
            f"{api_url}/api/audiences/query",
            token,
            workspace_id,
            {},
        )

        for audience in audiences:
            if audience.get("status") != "active":
                continue

            audience_name = audience["name"]
            audience_type = audience.get("type")
            if not is_exclusion_audience(audience_type, audience_name):
                continue

            connection_id = audience.get("connection") or audience.get("source")
            connection_name = connection_names.get(connection_id, connection_id)
            if connection_name in EXCLUDED_SOURCES:
                continue

            model = fetch_model(
                api_url,
                token,
                workspace_id,
                audience.get("config", {}).get("model"),
            )
            record = build_exclusion_record(audience, connection_name, model)

            if is_visitor_exclusion_name(audience_name):
                visitor_names_by_source.setdefault(connection_name, []).append(audience_name)
            elif is_purchaser_exclusion_name(audience_name):
                purchaser_names_by_source.setdefault(connection_name, []).append(audience_name)
            else:
                other_exclusions.append(record)

        if not other_exclusions and not visitor_names_by_source and not purchaser_names_by_source:
            continue

        sort_exclusions_by_source(visitor_names_by_source)
        sort_exclusions_by_source(purchaser_names_by_source)

        workspace_data.append(
            {
                "workspace.name": workspace_name,
                "workspace.id": workspace_id,
                "other_exclusions": other_exclusions,
                "visitor_names_by_source": visitor_names_by_source,
                "purchaser_names_by_source": purchaser_names_by_source,
            }
        )

    return workspace_data


def build_other_exclusions_overview(
    api_url: str,
    token: str,
    workspace_filter: set[str] | None = None,
) -> tuple[list[dict], list[dict], pd.DataFrame]:
    workspace_entries: list[dict] = []
    standard_only_entries: list[dict] = []
    rows: list[dict] = []

    for workspace in collect_workspace_exclusions(api_url, token, workspace_filter):
        visitor_names_by_source = workspace["visitor_names_by_source"]
        purchaser_names_by_source = workspace["purchaser_names_by_source"]
        other_exclusions = workspace["other_exclusions"]

        if not other_exclusions:
            standard_only_entries.append(
                {
                    "workspace.name": workspace["workspace.name"],
                    "workspace.id": workspace["workspace.id"],
                    "visitor_names_by_source": visitor_names_by_source,
                    "purchaser_names_by_source": purchaser_names_by_source,
                }
            )
            continue

        workspace_rows: list[dict] = []
        for exclusion in sorted(other_exclusions, key=lambda row: row["audience.name"]):
            source = exclusion["audience.source"]
            row = {
                "workspace.name": workspace["workspace.name"],
                **exclusion,
                VISITOR_EXCLUSIONS_COLUMN: format_exclusions_for_source(
                    visitor_names_by_source,
                    source,
                ),
                PURCHASER_EXCLUSIONS_COLUMN: format_exclusions_for_source(
                    purchaser_names_by_source,
                    source,
                ),
            }
            workspace_rows.append(row)
            rows.append(row)

        workspace_entries.append(
            {
                "workspace.name": workspace["workspace.name"],
                "workspace.id": workspace["workspace.id"],
                "rows": workspace_rows,
            }
        )

    table = (
        pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
        if rows
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )
    return workspace_entries, standard_only_entries, table


def format_markdown_value(value: object) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def format_exclusions_list(value: str) -> list[str]:
    if not value:
        return []
    return [name.strip() for name in value.split(";") if name.strip()]


def append_exclusions_list_lines(
    lines: list[str],
    label: str,
    names: list[str],
    indent: str = "   ",
    sub_indent: str = "     ",
) -> None:
    if names:
        lines.append(f"{indent}- **{label}:**")
        for name in names:
            lines.append(f"{sub_indent}- {name}")
    else:
        lines.append(f"{indent}- **{label}:** -")


def append_exclusions_by_source_lines(
    lines: list[str],
    heading: str,
    exclusions_by_source: dict[str, list[str]],
) -> None:
    lines.append(f"### {heading}")
    lines.append("")
    if not exclusions_by_source:
        lines.append("_None_")
        lines.append("")
        return
    for source in sorted(exclusions_by_source):
        lines.append(f"- **{source}**")
        for name in exclusions_by_source[source]:
            lines.append(f"  - {name}")
    lines.append("")


def build_markdown(
    workspace_entries: list[dict],
    standard_only_entries: list[dict],
) -> str:
    row_count = sum(len(entry["rows"]) for entry in workspace_entries)
    visitor_only_count = sum(
        len(names)
        for entry in standard_only_entries
        for names in entry["visitor_names_by_source"].values()
    )
    purchaser_only_count = sum(
        len(names)
        for entry in standard_only_entries
        for names in entry["purchaser_names_by_source"].values()
    )
    lines = [
        "# Other Exclusions Overview",
        "",
        "Other exclusions with existing visitor and purchaser exclusions on the same source.",
        "",
        f"- **Workspaces with other exclusions:** {len(workspace_entries)}",
        f"- **Other exclusions:** {row_count}",
        f"- **Workspaces with only visitor/purchaser exclusions:** {len(standard_only_entries)}",
        "",
    ]

    row_fields = [
        ("audience.id", "audience.id"),
        ("audience.name", "audience.name"),
        ("audience.source", "audience.source"),
        ("audience.targetingOutlookDays", "audience.targetingOutlookDays"),
        ("audience.audienceSizePercentage", "audience.audienceSizePercentage"),
    ]

    for workspace in workspace_entries:
        lines.extend(
            [
                f"## {workspace['workspace.name']}",
                "",
                f"- **workspace.id:** {workspace['workspace.id']}",
                "",
            ]
        )
        for index, row in enumerate(workspace["rows"], start=1):
            lines.append(f"{index}. **{row['audience.name']}**")
            for key, label in row_fields:
                if key == "audience.name":
                    continue
                lines.append(f"   - **{label}:** {format_markdown_value(row.get(key))}")

            append_exclusions_list_lines(
                lines,
                VISITOR_EXCLUSIONS_COLUMN,
                format_exclusions_list(row.get(VISITOR_EXCLUSIONS_COLUMN, "")),
            )
            append_exclusions_list_lines(
                lines,
                PURCHASER_EXCLUSIONS_COLUMN,
                format_exclusions_list(row.get(PURCHASER_EXCLUSIONS_COLUMN, "")),
            )
            lines.append("")

    lines.extend(
        [
            "# Customers with only Visitor or Purchaser Exclusions",
            "",
            f"- **Workspaces:** {len(standard_only_entries)}",
            f"- **Visitor exclusions:** {visitor_only_count}",
            f"- **Purchaser exclusions:** {purchaser_only_count}",
            "",
        ]
    )

    for workspace in standard_only_entries:
        lines.extend(
            [
                f"## {workspace['workspace.name']}",
                "",
            ]
        )
        append_exclusions_by_source_lines(
            lines,
            "Visitor Exclusions",
            workspace["visitor_names_by_source"],
        )
        append_exclusions_by_source_lines(
            lines,
            "Purchaser Exclusions",
            workspace["purchaser_names_by_source"],
        )

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a per-workspace overview of other exclusions with "
            "existing visitor exclusions for the same source."
        )
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=OUTPUT_JSON_PATH,
        help="Path for JSON output",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=OUTPUT_CSV_PATH,
        help="Path for CSV output",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=OUTPUT_MD_PATH,
        help="Path for markdown output",
    )
    parser.add_argument(
        "--workspace",
        action="append",
        dest="workspaces",
        metavar="NAME",
        help="Limit to one or more workspace names",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(SCRIPT_DIR / ".env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logging.info("Logging to %s", log_path)

    api_url = os.environ["TARGETING_URL"].rstrip("/")
    token = os.environ["API_SERVICE_TOKEN"]
    workspace_filter = normalize_workspace_filter(args.workspaces)

    if workspace_filter:
        logging.info("Workspace filter: %s", ", ".join(sorted(workspace_filter)))

    logging.info("Querying workspaces for other exclusion overview...")
    overview, standard_only, table = build_other_exclusions_overview(
        api_url, token, workspace_filter
    )

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(
            {
                "other_exclusions": overview,
                "visitor_or_purchaser_only": standard_only,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    table.to_csv(args.output_csv, index=False)
    args.output_md.write_text(build_markdown(overview, standard_only), encoding="utf-8")

    logging.info(
        "Found %s workspace(s) with %s other exclusion row(s) and "
        "%s visitor/purchaser-only workspace(s).",
        len(overview),
        len(table),
        len(standard_only),
    )
    logging.info("Saved JSON to %s", args.output_json)
    logging.info("Saved CSV to %s", args.output_csv)
    logging.info("Saved markdown to %s", args.output_md)


if __name__ == "__main__":
    main()
