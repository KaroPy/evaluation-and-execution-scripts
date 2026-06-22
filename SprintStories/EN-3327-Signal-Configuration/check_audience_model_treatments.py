"""
Audit active and new audiences for treatments and model type.

Extracts audience.id, audience.name, workspace, model.id, model.type, and
treatments count, then labels each row:
  - treatments is 0 or None -> add treatments
  - treatmentSyncStrategy != campaignBased -> wrong_treatment_sync
  - treatments > 0 and model.type == conversion -> check causal model
  - otherwise -> ok

Exclusions are ignored.

For rows labeled "check causal model", runs one Databricks SQL query per
workspace (only when at least one audience needs a causal check) combining
innkeepr_databricks.<workspace.id>.features_view_30_outlook_train and
features_view_365_outlook_train: optionally filter by created >= cut_date
(yyyy-mm-dd), deduplicate by session, treatment, conv_name, then group by
treatment and conv_name and count distinct sessions. Results are aggregated
per audience using audience.treatments and filtered to
audience.goal.conversionEvents.
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
from dotenv import load_dotenv

from check_signal_configuration import (
    EXCLUDED_SOURCES,
    api_post,
    fetch_model,
    query_all_pages,
)
from src.databricks_sql_client import DatabricksSQLClient
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_CAUSAL_CHECKS_DIR as CAUSAL_CHECKS_DIR,
)
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR,
    SCRIPT_DIR,
)
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_LOGS_DIR as LOGS_DIR,
)

OUTPUT_CSV_PATH = AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_audit.csv"
OUTPUT_MD_PATH = AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_audit.md"
OUTPUT_CAUSAL_RESULTS_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_causal_results.csv"
)
CAUSAL_RESULTS_COLUMNS = [
    "workspace.name",
    "audience.id",
    "audience.name",
    "treatment_conv_count.total",
    "potential sync bug",
    "audience.source",
    "audience.source.urlCampaignParam",
    "audience.source.urlTrackingParam",
    "audience.treatmentSyncStrategy",
    "label",
    "model.type",
    "audience.goal",
    "audience.goal.name",
    "audience.goal.conversionEvents",
    "audience.treatments",
    "workspace.id",
]
DATABRICKS_CATALOG = "innkeepr_databricks"
FEATURES_TABLE_SUFFIXES = (
    "features_view_30_outlook_train",
    "features_view_365_outlook_train",
)
LABEL_CHECK_CAUSAL = "check causal model"
LABEL_WRONG_TREATMENT_SYNC = "wrong_treatment_sync"
EXPECTED_TREATMENT_SYNC_STRATEGY = "campaignBased"
AUDIENCE_STATUSES = frozenset({"active", "new"})
CREATED_DATE_FORMAT = "%Y-%m-%d"


def causal_check_output_path(workspace_name: str, workspace_id: str) -> Path:
    safe_name = re.sub(r"[^\w\-.]+", "_", workspace_name).strip("_") or "workspace"
    return CAUSAL_CHECKS_DIR / f"{safe_name}_{workspace_id}_treatment_conv_session_counts.csv"


def resolve_causal_check_csv_path(workspace_name: str, workspace_id: str) -> Path | None:
    output_path = causal_check_output_path(workspace_name, workspace_id)
    if output_path.exists():
        return output_path
    legacy_path = CAUSAL_CHECKS_DIR / f"{workspace_id}_treatment_conv_session_counts.csv"
    if legacy_path.exists():
        return legacy_path
    return None


def load_causal_check_from_csv(workspace_name: str, workspace_id: str) -> pd.DataFrame:
    output_path = resolve_causal_check_csv_path(workspace_name, workspace_id)
    if output_path is None:
        raise FileNotFoundError(
            f"No cached causal check CSV for workspace {workspace_name} ({workspace_id})"
        )
    return pd.read_csv(output_path)


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"audience_model_treatments_{stamp}.log"


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.getLogger("databricks").setLevel(logging.WARNING)

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


def is_exclusion_audience(audience_type: str | None, audience_name: str) -> bool:
    return audience_type == "exclusion" or "Exclusion" in audience_name


def needs_causal_check(treatments_count: int | None, model_type: str | None) -> bool:
    return bool(treatments_count) and model_type == "conversion"


def label_audience(
    treatments_count: int | None,
    model_type: str | None,
    treatment_sync_strategy: str | None = None,
) -> str:
    if (
        treatment_sync_strategy is not None
        and treatment_sync_strategy != EXPECTED_TREATMENT_SYNC_STRATEGY
    ):
        return LABEL_WRONG_TREATMENT_SYNC
    if treatments_count is None or treatments_count == 0:
        return "add treatments"
    if model_type == "conversion":
        return LABEL_CHECK_CAUSAL
    return "ok"


def build_features_table_name(workspace_id: str, table_suffix: str) -> str:
    return f"{DATABRICKS_CATALOG}.{workspace_id}.{table_suffix}"


def get_treatments_list(config: dict) -> list:
    treatments = config.get("treatments")
    if not treatments:
        return []
    return list(treatments)


def fetch_goal(
    api_url: str,
    token: str,
    workspace_id: str,
    goal_id: str | None,
    cache: dict[tuple[str, str], dict | None],
) -> dict | None:
    if not goal_id:
        return None
    cache_key = (workspace_id, goal_id)
    if cache_key not in cache:
        goals = query_all_pages(
            f"{api_url}/api/goals/query",
            token,
            workspace_id,
            {"id": goal_id},
        )
        cache[cache_key] = goals[0] if goals else None
    return cache[cache_key]


def normalize_conversion_events(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item)]
    return [str(value)] if value else []


def normalize_cut_date(cut_date: str | datetime | None) -> str | None:
    if cut_date is None:
        return None
    if isinstance(cut_date, datetime):
        return cut_date.strftime(CREATED_DATE_FORMAT)
    return datetime.strptime(str(cut_date), CREATED_DATE_FORMAT).strftime(CREATED_DATE_FORMAT)


def build_causal_check_sql(
    workspace_id: str,
    cut_date: str | datetime | None = None,
) -> str:
    cut_date_sql = normalize_cut_date(cut_date)
    table_selects = "\n  UNION ALL\n".join(
        f"  SELECT session, treatment, conv_name, created\n"
        f"  FROM {build_features_table_name(workspace_id, table_suffix)}"
        for table_suffix in FEATURES_TABLE_SUFFIXES
    )
    if cut_date_sql:
        deduped_sql = (
            f"  SELECT DISTINCT session, treatment, conv_name\n"
            f"  FROM combined\n"
            f"  WHERE to_date(created) >= DATE '{cut_date_sql}'"
        )
    else:
        deduped_sql = "  SELECT DISTINCT session, treatment, conv_name\n  FROM combined"
    return f"""
WITH combined AS (
{table_selects}
),
deduped AS (
{deduped_sql}
)
SELECT treatment, conv_name, COUNT(DISTINCT session) AS session_count
FROM deduped
GROUP BY treatment, conv_name
ORDER BY conv_name, treatment
""".strip()


def has_databricks_sql_config() -> bool:
    return bool(
        os.environ.get("DATABRICKS_HOST")
        and (os.environ.get("DATABRICKS_WAREHOUSE_ID") or os.environ.get("DATABRICKS_HTTP_PATH"))
    )


def query_causal_check_via_sql(
    workspace_id: str,
    cut_date: str | datetime | None = None,
) -> pd.DataFrame:
    client = DatabricksSQLClient()
    sql_statement = build_causal_check_sql(workspace_id, cut_date=cut_date)
    with client.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
            return cursor.fetchall_arrow().to_pandas()


def query_causal_check(
    workspace_id: str,
    cut_date: str | datetime | None = None,
) -> tuple[pd.DataFrame, str]:
    if not has_databricks_sql_config():
        raise RuntimeError(
            "Databricks SQL config required: set DATABRICKS_HOST and "
            "DATABRICKS_WAREHOUSE_ID (or DATABRICKS_HTTP_PATH)"
        )
    return query_causal_check_via_sql(workspace_id, cut_date=cut_date), "databricks_sql"


def format_causal_check_error(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        return type(exc).__name__
    if len(message) > 300:
        return message[:300] + "..."
    return message


def normalize_treatments_list(treatments: object) -> list:
    if treatments is None or (isinstance(treatments, float) and pd.isna(treatments)):
        return []
    if isinstance(treatments, list):
        return treatments
    return list(treatments)


def filter_causal_check_for_audience(
    result: pd.DataFrame,
    treatments: list,
    conversion_events: list[str] | None = None,
) -> pd.DataFrame:
    if result.empty or not treatments:
        return result.iloc[0:0].copy()
    filtered = result[result["treatment"].isin(treatments)]
    if conversion_events:
        filtered = filtered[filtered["conv_name"].isin(conversion_events)]
    return filtered.reset_index(drop=True)


def treatments_missing_in_data(result: pd.DataFrame, treatments: list) -> list[str]:
    if result.empty:
        return list(treatments)
    treatments_in_data = set(
        result.loc[result["treatment"].isin(treatments), "treatment"].dropna().unique()
    )
    return [treatment_id for treatment_id in treatments if treatment_id not in treatments_in_data]


def fetch_treatment_status(
    api_url: str,
    token: str,
    workspace_id: str,
    treatment_id: str,
) -> str | None:
    treatments = query_all_pages(
        f"{api_url}/api/treatments/query",
        token,
        workspace_id,
        {"id": treatment_id},
    )
    if not treatments:
        return None
    treatment = treatments[0]
    return treatment.get("properties", {}).get("status") or treatment.get("status")


def treatment_conv_counts_for_audience(
    result: pd.DataFrame,
    treatments: list,
    conversion_events: list[str] | None = None,
) -> dict[str, int]:
    filtered = filter_causal_check_for_audience(result, treatments, conversion_events)
    if filtered.empty:
        return dict.fromkeys(treatments, 0)
    counts = filtered.groupby("treatment")["session_count"].sum().astype(int).to_dict()
    return {treatment_id: int(counts.get(treatment_id, 0)) for treatment_id in treatments}


def label_treatment_conv(conv_count: int, status: str | None) -> str:
    if conv_count == 0 and status == "active":
        return "error"
    return "fine"


def build_treatment_conv_summary(
    treatments: list,
    conv_counts: dict[str, int],
    get_status,
) -> dict[str, dict]:
    summary: dict[str, dict] = {}
    for treatment_id in treatments:
        conv_count = int(conv_counts.get(treatment_id, 0))
        status = get_status(treatment_id)
        summary[treatment_id] = {
            "conv_count": conv_count,
            "status": status,
            "label": label_treatment_conv(conv_count, status),
        }
    return summary


def build_causal_treatment_results(
    table: pd.DataFrame,
    workspace_results: dict[str, pd.DataFrame],
    api_url: str,
    token: str,
) -> pd.DataFrame:
    rows: list[dict] = []
    status_cache: dict[tuple[str, str], str | None] = {}
    causal_rows = table.loc[
        table.apply(
            lambda row: needs_causal_check(
                row.get("audience.treatments.count"),
                row.get("model.type"),
            ),
            axis=1,
        )
    ]

    for _, row in causal_rows.iterrows():
        workspace_id = row["workspace.id"]
        workspace_name = row["workspace.name"]
        audience_id = row["audience.id"]
        treatments = normalize_treatments_list(row.get("audience.treatments"))
        conversion_events = normalize_conversion_events(row.get("audience.goal.conversionEvents"))
        result = workspace_results.get(workspace_id, pd.DataFrame())
        conv_counts = treatment_conv_counts_for_audience(result, treatments, conversion_events)
        treatment_conv_count_total = sum(conv_counts.values())

        def get_treatment_status(treatment_id: str) -> str | None:
            cache_key = (workspace_id, treatment_id)
            if cache_key not in status_cache:
                status_cache[cache_key] = fetch_treatment_status(
                    api_url, token, workspace_id, treatment_id
                )
            return status_cache[cache_key]

        treatment_summary = build_treatment_conv_summary(
            treatments, conv_counts, get_treatment_status
        )
        error_treatments = [
            treatment_id
            for treatment_id, details in treatment_summary.items()
            if details["label"] == "error"
        ]
        if error_treatments:
            logging.info(
                "Audience %s active treatments with 0 conv sessions: %s",
                audience_id,
                ", ".join(error_treatments),
            )

        rows.append(
            {
                "workspace.name": workspace_name,
                "audience.id": audience_id,
                "audience.name": row["audience.name"],
                "treatment_conv_count.total": treatment_conv_count_total,
                "potential sync bug": "potential sync bug" if error_treatments else "",
                "audience.source": row.get("audience.source"),
                "audience.source.urlCampaignParam": row.get("audience.source.urlCampaignParam"),
                "audience.source.urlTrackingParam": row.get("audience.source.urlTrackingParam"),
                "audience.treatmentSyncStrategy": row.get("audience.treatmentSyncStrategy"),
                "label": row.get("label"),
                "model.type": row["model.type"],
                "audience.goal": row.get("audience.goal"),
                "audience.goal.name": row.get("audience.goal.name"),
                "audience.goal.conversionEvents": conversion_events,
                "audience.treatments": treatment_summary,
                "workspace.id": workspace_id,
            }
        )

    if not rows:
        return pd.DataFrame(columns=CAUSAL_RESULTS_COLUMNS)

    return pd.DataFrame(rows)[CAUSAL_RESULTS_COLUMNS]


def summarize_causal_check_for_audience(
    result: pd.DataFrame,
    treatments: list,
    conversion_events: list[str] | None = None,
) -> dict:
    filtered = filter_causal_check_for_audience(result, treatments, conversion_events)
    if filtered.empty:
        return {
            "causal.treatment_conv_groups": 0,
            "causal.session_total": 0,
        }
    return {
        "causal.treatment_conv_groups": len(filtered),
        "causal.session_total": int(filtered["session_count"].sum()),
    }


def enrich_causal_checks(
    table: pd.DataFrame,
    read_existing_data: bool = False,
    cut_date: str | datetime | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    enriched = table.copy()
    for column in [
        "causal.check.method",
        "causal.check.status",
        "causal.check.error",
        "causal.treatment_conv_groups",
        "causal.session_total",
        "causal.check.output_path",
    ]:
        if column not in enriched.columns:
            enriched[column] = None

    causal_rows = enriched.loc[
        enriched.apply(
            lambda row: needs_causal_check(
                row.get("audience.treatments.count"),
                row.get("model.type"),
            ),
            axis=1,
        )
    ]
    if causal_rows.empty:
        return enriched, {}

    workspace_ids = causal_rows["workspace.id"].dropna().unique()
    workspace_names = causal_rows.groupby("workspace.id")["workspace.name"].first().to_dict()
    CAUSAL_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    workspace_results: dict[str, pd.DataFrame] = {}
    workspace_errors: dict[str, str] = {}
    workspace_methods: dict[str, str] = {}
    workspace_output_paths: dict[str, str] = {}

    for workspace_id in workspace_ids:
        workspace_name = workspace_names.get(workspace_id, "workspace")
        output_path = causal_check_output_path(workspace_name, workspace_id)
        try:
            if read_existing_data:
                cached_path = resolve_causal_check_csv_path(workspace_name, workspace_id)
                if cached_path is None:
                    raise FileNotFoundError(
                        f"No cached causal check CSV for workspace "
                        f"{workspace_name} ({workspace_id})"
                    )
                result = load_causal_check_from_csv(workspace_name, workspace_id)
                method = "cached_csv"
                output_path = cached_path
            else:
                result, method = query_causal_check(workspace_id, cut_date=cut_date)
                result.to_csv(output_path, index=False)
            workspace_results[workspace_id] = result
            workspace_methods[workspace_id] = method
            workspace_output_paths[workspace_id] = str(output_path)
            logging.info(
                "Causal check for workspace %s via %s: %s treatment/conv_name groups",
                workspace_id,
                method,
                len(result),
            )
        except Exception as exc:
            logging.warning("Causal check failed for workspace %s: %s", workspace_id, exc)
            workspace_errors[workspace_id] = format_causal_check_error(exc)

    for index, row in enriched.iterrows():
        if not needs_causal_check(
            row.get("audience.treatments.count"),
            row.get("model.type"),
        ):
            continue

        workspace_id = row["workspace.id"]
        if workspace_id in workspace_errors:
            enriched.at[index, "causal.check.status"] = "error"
            enriched.at[index, "causal.check.error"] = workspace_errors[workspace_id]
            continue

        result = workspace_results.get(workspace_id)
        if result is None:
            continue

        treatments = normalize_treatments_list(row.get("audience.treatments"))
        conversion_events = normalize_conversion_events(row.get("audience.goal.conversionEvents"))
        summary = summarize_causal_check_for_audience(result, treatments, conversion_events)
        enriched.at[index, "causal.check.method"] = workspace_methods[workspace_id]
        enriched.at[index, "causal.check.status"] = "ok"
        enriched.at[index, "causal.check.error"] = None
        enriched.at[index, "causal.check.output_path"] = workspace_output_paths[workspace_id]
        for key, value in summary.items():
            enriched.at[index, key] = value

        logging.info(
            "Audience %s (%s): %s treatment/conv_name groups, %s sessions",
            row["audience.name"],
            row["audience.id"],
            summary["causal.treatment_conv_groups"],
            summary["causal.session_total"],
        )

    return enriched, workspace_results


def build_audit_table(
    api_url: str,
    token: str,
    workspace_filter: set[str] | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
    workspaces = api_post(f"{api_url}/api/core/workspaces/query", token, {"content": {}})

    for workspace in workspaces:
        workspace_name = workspace["name"]
        if workspace_filter and workspace_name not in workspace_filter:
            continue

        workspace_id = workspace["id"]
        connections = api_post(
            f"{api_url}/api/connections/query",
            token,
            {"content": {}, "context": {"workspaceId": workspace_id}},
        )
        connections_by_id = {conn.get("_id") or conn.get("id"): conn for conn in connections}
        goal_cache: dict[tuple[str, str], dict | None] = {}

        audiences = query_all_pages(
            f"{api_url}/api/audiences/query",
            token,
            workspace_id,
            {},
        )

        for audience in audiences:
            if audience.get("status") not in AUDIENCE_STATUSES:
                continue

            audience_id = audience["id"]
            audience_name = audience["name"]
            audience_type = audience.get("type")
            if is_exclusion_audience(audience_type, audience_name):
                continue

            connection_id = audience.get("connection") or audience.get("source")
            connection = connections_by_id.get(connection_id, {})
            connection_name = connection.get("name", connection_id)
            if connection_name in EXCLUDED_SOURCES:
                continue

            connection_options = connection.get("options") or {}
            config = audience.get("config", {})
            model_id = config.get("model")
            model = fetch_model(api_url, token, workspace_id, model_id)
            model_type = model.get("type")
            treatments = get_treatments_list(config)
            treatments_count = len(treatments)
            goal_id = config.get("goal") or model.get("goal")
            goal = fetch_goal(api_url, token, workspace_id, goal_id, goal_cache)
            conversion_events = list(goal.get("conversionEvents") or []) if goal else []
            treatment_sync_strategy = config.get("treatmentSyncStrategy")

            rows.append(
                {
                    "workspace.name": workspace_name,
                    "workspace.id": workspace_id,
                    "audience.id": audience_id,
                    "audience.name": audience_name,
                    "audience.type": audience_type,
                    "audience.source": connection_name,
                    "audience.source.urlCampaignParam": connection_options.get("urlCampaignParam"),
                    "audience.source.urlTrackingParam": connection_options.get("urlTrackingParam"),
                    "audience.treatmentSyncStrategy": treatment_sync_strategy,
                    "model.id": model_id,
                    "model.type": model_type,
                    "audience.goal": goal_id,
                    "audience.goal.name": goal.get("name") if goal else None,
                    "audience.goal.conversionEvents": conversion_events,
                    "audience.treatments": treatments,
                    "audience.treatments.count": treatments_count,
                    "label": label_audience(
                        treatments_count,
                        model_type,
                        treatment_sync_strategy,
                    ),
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(
            ["label", "workspace.name", "audience.name"],
            na_position="last",
        )
        .reset_index(drop=True)
    )


def build_markdown(table: pd.DataFrame) -> str:
    label_counts = table["label"].value_counts().to_dict()
    summary_lines = [
        "# Audience Model / Treatments Audit",
        "",
        "Exclusions are ignored.",
        "",
        f"- **Total audiences (active + new):** {len(table)}",
    ]
    for label, count in sorted(label_counts.items()):
        summary_lines.append(f"- **{label}:** {count}")
    summary_lines.extend(["", "## Results", ""])

    headers = [
        "Status",
        "Workspace",
        "`workspace.id`",
        "`audience.id`",
        "Audience",
        "Source",
        "`model.id`",
        "`model.type`",
        "Treatments",
        "Label",
        "Causal groups",
        "Causal sessions",
        "Causal status",
    ]
    summary_lines.append("| " + " | ".join(headers) + " |")
    summary_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    sorted_table = table.sort_values(
        ["workspace.name", "audience.name"],
        na_position="last",
    )

    status_icon = {
        "add treatments": "❌",
        LABEL_CHECK_CAUSAL: "⚠️",
        LABEL_WRONG_TREATMENT_SYNC: "❌",
        "ok": "✅",
    }
    for _, row in sorted_table.iterrows():
        label = row["label"]
        icon = status_icon.get(label, "")
        summary_lines.append(
            "| {icon} | {workspace} | {workspace_id} | {audience_id} | {audience_name} | "
            "{source} | {model_id} | {model_type} | {treatments} | {label} | "
            "{causal_groups} | {causal_sessions} | {causal_status} |".format(
                icon=icon,
                workspace=row["workspace.name"],
                workspace_id=row.get("workspace.id", "-"),
                audience_id=row["audience.id"],
                audience_name=row["audience.name"],
                source=row["audience.source"],
                model_id=row["model.id"] if pd.notna(row["model.id"]) else "-",
                model_type=row["model.type"] if pd.notna(row["model.type"]) else "-",
                treatments=row["audience.treatments.count"],
                label=label,
                causal_groups=row.get("causal.treatment_conv_groups", "-"),
                causal_sessions=row.get("causal.session_total", "-"),
                causal_status=row.get("causal.check.status", "-"),
            )
        )

    return "\n".join(summary_lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Audit active and new audiences for treatments count and model type labeling.")
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
        "--output-causal-results",
        type=Path,
        default=OUTPUT_CAUSAL_RESULTS_PATH,
        help="Path for per-treatment causal results CSV",
    )
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to one or more customer names (workspace.name), e.g. Rosental",
    )
    parser.add_argument(
        "--workspace",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Alias for --customer",
    )
    parser.add_argument(
        "--skip-causal-check",
        action="store_true",
        help="Skip Databricks causal checks for conversion models",
    )
    parser.add_argument(
        "--read-existing-data",
        action="store_true",
        help=(
            "Load causal check data from existing CSV files in "
            "audience_model_treatments/data/causal_model_checks instead of querying Databricks"
        ),
    )
    parser.add_argument(
        "--cut-date",
        metavar="DATE",
        help=f"Only include rows with created >= DATE (format: {CREATED_DATE_FORMAT})",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help=(
            "Path for log output "
            "(default: audience_model_treatments/logs/audience_model_treatments_<timestamp>.log)"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    load_dotenv(SCRIPT_DIR / ".env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logging.info("Logging to %s", log_path)

    api_url = os.environ["TARGETING_URL"].rstrip("/")
    token = os.environ["API_SERVICE_TOKEN"]
    customer_filter = normalize_customer_filter(args.customers)

    if customer_filter:
        logging.info("Filtering to customers: %s", ", ".join(sorted(customer_filter)))

    logging.info("Querying workspaces and audiences with status active/new (exclusions ignored)...")
    table = build_audit_table(api_url, token, customer_filter)

    causal_audience_count = int(
        table.apply(
            lambda row: needs_causal_check(
                row.get("audience.treatments.count"),
                row.get("model.type"),
            ),
            axis=1,
        ).sum()
    )
    workspace_results: dict[str, pd.DataFrame] = {}
    if args.skip_causal_check:
        logging.info("Skipping Databricks causal checks.")
    elif causal_audience_count == 0:
        logging.info("No audiences require causal checks.")
    else:
        if args.read_existing_data:
            logging.info(
                "Loading cached causal checks for %s audiences from %s...",
                causal_audience_count,
                CAUSAL_CHECKS_DIR,
            )
        else:
            if args.cut_date:
                logging.info(
                    "Causal check created filter: created >= %s",
                    normalize_cut_date(args.cut_date),
                )
            logging.info(
                "Running Databricks causal checks for %s audiences...",
                causal_audience_count,
            )
        table, workspace_results = enrich_causal_checks(
            table,
            read_existing_data=args.read_existing_data,
            cut_date=args.cut_date,
        )
        causal_results = build_causal_treatment_results(table, workspace_results, api_url, token)
        args.output_causal_results.parent.mkdir(parents=True, exist_ok=True)
        causal_results_for_csv = causal_results.copy()
        json_columns = (
            "audience.treatments",
            "audience.goal.conversionEvents",
        )
        for column in json_columns:
            if column in causal_results_for_csv.columns:
                causal_results_for_csv[column] = causal_results_for_csv[column].apply(json.dumps)
        causal_results_for_csv = causal_results_for_csv[CAUSAL_RESULTS_COLUMNS]
        causal_results_for_csv.to_csv(args.output_causal_results, index=False)
        logging.info("Saved causal results to %s", args.output_causal_results)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(args.output_csv, index=False)
    args.output_md.write_text(build_markdown(table), encoding="utf-8")

    logging.info("Total audiences: %s", len(table))
    for label, count in table["label"].value_counts().items():
        logging.info("  %s: %s", label, count)
    logging.info("Saved CSV to %s", args.output_csv)
    logging.info("Saved markdown to %s", args.output_md)
    logging.info("Log saved to %s", log_path)


if __name__ == "__main__":
    main()
