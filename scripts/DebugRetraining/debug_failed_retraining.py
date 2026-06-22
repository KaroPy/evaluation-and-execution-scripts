"""
Debug failed k8-retraining Prefect flows from the last N days.

For each failed run:
  1. Resolve workspace and signal (audience) via targeting API
  2. Load the signal's configured model via models/query (fallback: newest for audience)
  3. Derive model release date from model.path
  4. Load excluded_dates from delta_share_events.monitoring.analytics_exclude_dates
     (ignore_training) and apply them in Databricks SQL filters
  5. Query Databricks SQL for treatment and conversion session counts since release
  6. Write a combined CSV report

Usage (from repo root):
    python scripts/DebugRetraining/debug_failed_retraining.py
    python scripts/DebugRetraining/debug_failed_retraining.py --days 7 --customer Junglueck
    python scripts/DebugRetraining/debug_failed_retraining.py --treatment-count-lookback-days 365
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import os
import re
import sys
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from pathlib import Path

import delta_sharing
import pandas as pd
import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
print(f"REPO_ROOT: {REPO_ROOT}")
EN3327_DIR = REPO_ROOT / "SprintStories/EN-3327-Signal-Configuration"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(EN3327_DIR))

from check_audience_model_treatments import (  # type: ignore[import-not-found]  # noqa: E402, I001
    DatabricksSQLClient,
    FEATURES_TABLE_SUFFIXES,
    build_features_table_name,
    fetch_goal,
    has_databricks_sql_config,
    normalize_conversion_events,
    normalize_cut_date,
    treatment_conv_counts_for_audience,
)
from check_signal_configuration import (  # type: ignore[import-not-found]  # noqa: E402
    api_post,
    fetch_model,
    query_all_pages,
    sanitize_workspace_name,
)
from general_functions.databricks_client import return_databricks_client  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"
DEFAULT_OUTPUT = DATA_DIR / "failed_retraining_debug.csv"
FLOW_NAME = "k8-retraining"
PREFECT_API_URL = "https://prefect.innkeepr.ai/api"
DELTA_SHARE_CATALOG = "delta_share_events"
ANALYTICS_EXCLUDE_DATES_TABLE = f"{DELTA_SHARE_CATALOG}.monitoring.analytics_exclude_dates"
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
CREATED_DATE_FORMAT = "%Y-%m-%d"


def format_date_for_prefect_api(date) -> str:
    date = pd.to_datetime(date)
    return datetime.fromisoformat(str(date)).strftime("%Y-%m-%dT%H:%M:%SZ")


def call_prefect_api(endpoint: str, json_data: dict) -> list | dict:
    secret = b64encode(os.environ["PREFECT_API_KEY"].encode("utf-8"))
    auth_header = f"Basic {secret.decode('utf-8')}"
    response = requests.post(
        f"{PREFECT_API_URL}{endpoint}",
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/json",
        },
        json=json_data,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"debug_failed_retraining_{stamp}.log"


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


def extract_date_from_path(path: str | None) -> str | None:
    if not path:
        return None
    matches = DATE_PATTERN.findall(path)
    return matches[-1] if matches else None


def normalize_excluded_date(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().strip("'\"")
    if not text:
        return None
    if DATE_PATTERN.fullmatch(text):
        return text
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime(CREATED_DATE_FORMAT)


def parse_excluded_dates(raw_value: object) -> list[str]:
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return []

    parsed_value = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed_value = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            parsed_value = [part.strip() for part in text.split(",") if part.strip()]

    if not isinstance(parsed_value, (list, tuple, set)):
        parsed_value = [parsed_value]

    dates: list[str] = []
    seen: set[str] = set()
    for item in parsed_value:
        normalized = normalize_excluded_date(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            dates.append(normalized)
    return sorted(dates)


def load_analytics_exclude_dates(logger: logging.Logger) -> pd.DataFrame:
    profile_path = return_databricks_client()
    table_path = f"{profile_path}#{ANALYTICS_EXCLUDE_DATES_TABLE}"
    logger.info("Loading excluded dates from %s", ANALYTICS_EXCLUDE_DATES_TABLE)
    df = delta_sharing.load_as_pandas(table_path)
    logger.info("Loaded excluded dates for %s workspaces/customers", len(df))
    return df


def build_exclude_dates_lookup(exclude_dates_df: pd.DataFrame) -> dict[str, list[str]]:
    lookup: dict[str, list[str]] = {}
    for _, row in exclude_dates_df.iterrows():
        dates = parse_excluded_dates(row.get("ignore_training"))
        workspace_id = row.get("workspace_id")
        customer = row.get("customer")
        if workspace_id and not (isinstance(workspace_id, float) and pd.isna(workspace_id)):
            lookup[str(workspace_id)] = dates
        if customer and not (isinstance(customer, float) and pd.isna(customer)):
            lookup[sanitize_workspace_name(str(customer))] = dates
    return lookup


def resolve_excluded_dates(
    lookup: dict[str, list[str]],
    workspace: dict | None,
    tenant: str | None,
) -> list[str]:
    if workspace:
        workspace_id = str(workspace["id"])
        if workspace_id in lookup:
            return lookup[workspace_id]
        workspace_key = sanitize_workspace_name(workspace["name"])
        if workspace_key in lookup:
            return lookup[workspace_key]
    if tenant and str(tenant).strip() in lookup:
        return lookup[str(tenant).strip()]
    return []


def build_created_date_filter(cut_date: str | None, excluded_dates: list[str]) -> str:
    clauses: list[str] = []
    cut_date_sql = normalize_cut_date(cut_date)
    if cut_date_sql:
        clauses.append(f"to_date(created) >= DATE '{cut_date_sql}'")
    if excluded_dates:
        exclude_literals = ", ".join(f"DATE '{date}'" for date in excluded_dates)
        clauses.append(f"to_date(created) NOT IN ({exclude_literals})")
    if not clauses:
        return ""
    return "\n  WHERE " + " AND ".join(clauses)


def query_failed_retraining_runs(
    start_time: datetime,
    end_time: datetime,
    logger: logging.Logger,
) -> pd.DataFrame:
    endpoint = "/flow_runs/filter"
    all_responses: list[dict] = []
    cursor = pd.to_datetime(start_time, utc=True)
    end = pd.to_datetime(end_time, utc=True)

    while cursor < end:
        chunk_end = min(cursor + timedelta(days=1), end)
        payload = {
            "deployments": {"name": {"like_": FLOW_NAME}},
            "sort": "START_TIME_ASC",
            "flow_runs": {
                "start_time": {
                    "after_": format_date_for_prefect_api(cursor),
                    "before_": format_date_for_prefect_api(chunk_end),
                },
                "state": {"type": {"any_": ["FAILED"]}},
            },
        }
        logger.info(
            "Querying failed %s runs from %s to %s",
            FLOW_NAME,
            cursor.isoformat(),
            chunk_end.isoformat(),
        )
        response = call_prefect_api(endpoint=endpoint, json_data=payload)
        all_responses.extend(response)
        cursor = chunk_end

    if not all_responses:
        return pd.DataFrame()

    runs = pd.json_normalize(all_responses)
    keep_columns = [
        col
        for col in [
            "id",
            "deployment_id",
            "name",
            "state_type",
            "start_time",
            "total_run_time",
            "parameters.tenant",
            "parameters.audience",
            "parameters.reset_lstm",
            "parameters.max_model_age_in_days",
        ]
        if col in runs.columns
    ]
    return runs[keep_columns].copy()


def fetch_failed_task_messages(flow_runs: pd.DataFrame, logger: logging.Logger) -> pd.Series:
    messages: dict[str, str] = {}
    for _, row in flow_runs.iterrows():
        flow_run_id = row["id"]
        deployment_id = row["deployment_id"]
        payload = {
            "flow_runs": {"id": {"any_": [flow_run_id]}},
            "deployments": {"id": {"any_": [deployment_id]}},
        }
        task_runs = call_prefect_api(endpoint="/task_runs/filter", json_data=payload)
        failed_messages = [
            task.get("state", {}).get("message", "")
            for task in task_runs
            if task.get("state_type") == "FAILED" and task.get("state", {}).get("message")
        ]
        messages[flow_run_id] = " | ".join(failed_messages[:3])
        logger.info("Fetched failure messages for flow run %s", flow_run_id)
    return flow_runs["id"].map(messages)


def build_workspace_by_tenant(api_url: str, token: str) -> dict[str, dict]:
    workspaces = api_post(f"{api_url}/api/core/workspaces/query", token, {"content": {}})
    return {sanitize_workspace_name(workspace["name"]): workspace for workspace in workspaces}


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def resolve_workspace(
    tenant: str | None,
    by_tenant: dict[str, dict],
) -> dict | None:
    if not tenant or (isinstance(tenant, float) and pd.isna(tenant)):
        return None
    return by_tenant.get(str(tenant).strip())


def fetch_signal(
    api_url: str,
    token: str,
    workspace_id: str,
    signal_id: str,
) -> dict | None:
    signals = query_all_pages(
        f"{api_url}/api/audiences/query",
        token,
        workspace_id,
        {"id": signal_id},
    )
    return signals[0] if signals else None


def fetch_model_for_signal(
    api_url: str,
    token: str,
    workspace_id: str,
    signal: dict | None,
    signal_id: str,
) -> dict | None:
    config = (signal or {}).get("config") or {}
    model_id = config.get("model")
    if model_id:
        model = fetch_model(api_url, token, workspace_id, model_id)
        return model or None

    models = query_all_pages(
        f"{api_url}/api/models/query",
        token,
        workspace_id,
        {"audience": signal_id},
    )
    if not models:
        models = query_all_pages(
            f"{api_url}/api/models/query",
            token,
            workspace_id,
            {"audienceId": signal_id},
        )
    audience_models = [model for model in models if signal_id in (model.get("path") or "")]
    if not audience_models:
        return None
    return max(audience_models, key=lambda model: model.get("created", ""))


def get_signal_treatments(config: dict) -> list[str]:
    treatments = config.get("treatments") or []
    return [str(item) for item in treatments if item]


def build_treatment_count_sql(
    workspace_id: str,
    treatments: list[str],
    cut_date: str | None = None,
    excluded_dates: list[str] | None = None,
    lookback_days: int | None = None,
) -> str:
    if lookback_days is not None:
        treatment_cut_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime(
            CREATED_DATE_FORMAT
        )
    else:
        treatment_cut_date = cut_date
    treatment_literals = ", ".join(sql_literal(treatment) for treatment in treatments)
    table_selects = "\n  UNION ALL\n".join(
        f"  SELECT session, treatment, conv_name, created\n"
        f"  FROM {build_features_table_name(workspace_id, table_suffix)}"
        for table_suffix in FEATURES_TABLE_SUFFIXES
    )
    created_filter = build_created_date_filter(treatment_cut_date, excluded_dates or [])
    return f"""
WITH combined AS (
{table_selects}
),
deduped AS (
  SELECT DISTINCT session, treatment, conv_name
  FROM combined{created_filter}
),
filtered AS (
  SELECT session, treatment, conv_name
  FROM deduped
  WHERE treatment IN ({treatment_literals})
)
SELECT
  COUNT(DISTINCT session) AS treatment_session_count,
  COUNT(DISTINCT CASE
    WHEN conv_name IS NOT NULL AND conv_name != '' THEN session
  END) AS treatment_with_any_conversion_count
FROM filtered
""".strip()


def build_conversion_match_sql(
    workspace_id: str,
    treatments: list[str],
    conversion_events: list[str],
    cut_date: str | None,
    excluded_dates: list[str] | None = None,
) -> str:
    treatment_literals = ", ".join(sql_literal(treatment) for treatment in treatments)
    conversion_literals = ", ".join(sql_literal(event) for event in conversion_events)
    table_selects = "\n  UNION ALL\n".join(
        f"  SELECT session, treatment, conv_name, created\n"
        f"  FROM {build_features_table_name(workspace_id, table_suffix)}"
        for table_suffix in FEATURES_TABLE_SUFFIXES
    )
    created_filter = build_created_date_filter(cut_date, excluded_dates or [])
    return f"""
WITH combined AS (
{table_selects}
),
deduped AS (
  SELECT DISTINCT session, treatment, conv_name
  FROM combined{created_filter}
),
filtered AS (
  SELECT session, treatment, conv_name
  FROM deduped
  WHERE treatment IN ({treatment_literals})
    AND conv_name IN ({conversion_literals})
)
SELECT COUNT(DISTINCT session) AS conversion_match_count
FROM filtered
""".strip()


def query_scalar_counts(sql_statement: str) -> dict[str, int]:
    client = DatabricksSQLClient()
    with client.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
            row = cursor.fetchone()
            if row is None:
                return {}
            columns = [column[0] for column in cursor.description]
            values = dict(zip(columns, row))
            return {key: int(values.get(key) or 0) for key in values}


def build_causal_check_sql(
    workspace_id: str,
    cut_date: str | None,
    excluded_dates: list[str] | None = None,
) -> str:
    table_selects = "\n  UNION ALL\n".join(
        f"  SELECT session, treatment, conv_name, created\n"
        f"  FROM {build_features_table_name(workspace_id, table_suffix)}"
        for table_suffix in FEATURES_TABLE_SUFFIXES
    )
    created_filter = build_created_date_filter(cut_date, excluded_dates or [])
    return f"""
WITH combined AS (
{table_selects}
),
deduped AS (
  SELECT DISTINCT session, treatment, conv_name
  FROM combined{created_filter}
)
SELECT treatment, conv_name, COUNT(DISTINCT session) AS session_count
FROM deduped
GROUP BY treatment, conv_name
ORDER BY conv_name, treatment
""".strip()


def query_workspace_causal_check(
    workspace_id: str,
    cut_date: str | None,
    excluded_dates: list[str],
    cache: dict[tuple[str, str | None, tuple[str, ...]], pd.DataFrame],
) -> pd.DataFrame:
    cache_key = (workspace_id, cut_date, tuple(excluded_dates))
    if cache_key not in cache:
        sql_statement = build_causal_check_sql(
            workspace_id,
            cut_date=cut_date,
            excluded_dates=excluded_dates,
        )
        cache[cache_key] = query_scalar_dataframe(sql_statement)
    return cache[cache_key]


def query_scalar_dataframe(sql_statement: str) -> pd.DataFrame:
    client = DatabricksSQLClient()
    with client.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
            return cursor.fetchall_arrow().to_pandas()


def compute_databricks_counts(
    workspace_id: str,
    treatments: list[str],
    conversion_events: list[str],
    cut_date: str | None,
    excluded_dates: list[str],
    causal_cache: dict[tuple[str, str | None, tuple[str, ...]], pd.DataFrame],
    treatment_count_lookback_days: int | None = None,
) -> dict[str, int | None]:
    if not treatments:
        logging.info(f"No treatments for signal {workspace_id}")
        return {
            "treatment.session_count": 0,
            "conversion.match_count": 0,
            "treatment_conv_count.total": 0,
        }
    if not has_databricks_sql_config():
        logging.info(f"No Databricks SQL config for signal {workspace_id}")
        return {
            "treatment.session_count": None,
            "conversion.match_count": None,
            "treatment_conv_count.total": None,
        }

    treatment_sql = build_treatment_count_sql(
        workspace_id,
        treatments,
        cut_date=cut_date,
        excluded_dates=excluded_dates,
        lookback_days=treatment_count_lookback_days,
    )
    treatment_counts = query_scalar_counts(treatment_sql)
    print(f"treatment_counts: {treatment_counts}")

    conversion_match_count = 0
    if conversion_events:
        conversion_sql = build_conversion_match_sql(
            workspace_id,
            treatments,
            conversion_events,
            cut_date,
            excluded_dates=excluded_dates,
        )
        conversion_match_count = query_scalar_counts(conversion_sql).get(
            "conversion_match_count", 0
        )

    causal_result = query_workspace_causal_check(
        workspace_id,
        cut_date,
        excluded_dates,
        causal_cache,
    )
    conv_counts = treatment_conv_counts_for_audience(
        causal_result,
        treatments,
        conversion_events or None,
    )

    return {
        "treatment.session_count": treatment_counts.get("treatment_session_count", 0),
        "conversion.match_count": conversion_match_count,
        "treatment_conv_count.total": sum(conv_counts.values()),
    }


def build_result_row(
    flow_row: pd.Series,
    workspace: dict | None,
    signal: dict | None,
    model: dict | None,
    goal: dict | None,
    failure_message: str | None,
    excluded_dates: list[str],
    databricks_counts: dict[str, int | None],
) -> dict:
    config = (signal or {}).get("config") or {}
    treatments = get_signal_treatments(config)
    conversion_events = list((goal or {}).get("conversionEvents") or [])
    model_path = (model or {}).get("path")
    model_release_date = extract_date_from_path(model_path) or (model or {}).get("created")

    return {
        "flow_run.id": flow_row.get("id"),
        "flow_run.start_time": flow_row.get("start_time"),
        "flow_run.failure.message": failure_message,
        "parameters.tenant": flow_row.get("parameters.tenant"),
        "workspace.name": workspace["name"] if workspace else None,
        "workspace.id": workspace["id"] if workspace else None,
        "signal.id": flow_row.get("parameters.audience"),
        "signal.name": (signal or {}).get("name"),
        "signal.type": (signal or {}).get("type"),
        "signal.status": (signal or {}).get("status"),
        "signal.config.goal": config.get("goal") or (model or {}).get("goal"),
        "signal.goal.name": (goal or {}).get("name"),
        "signal.goal.conversionEvents": conversion_events,
        "signal.treatments": treatments,
        "signal.treatments.count": len(treatments),
        "model.id": config.get("model") or (model or {}).get("id"),
        "model.type": (model or {}).get("type"),
        "model.created": (model or {}).get("created"),
        "model.path": model_path,
        "model.release_date": model_release_date,
        "excluded_dates": excluded_dates,
        "treatment.session_count": databricks_counts.get("treatment.session_count"),
        "conversion.match_count": databricks_counts.get("conversion.match_count"),
        "treatment_conv_count.total": databricks_counts.get("treatment_conv_count.total"),
    }


def build_report_table(
    flow_runs: pd.DataFrame,
    api_url: str,
    token: str,
    customer_filter: set[str] | None,
    exclude_dates_lookup: dict[str, list[str]],
    logger: logging.Logger,
    treatment_count_lookback_days: int | None = None,
) -> pd.DataFrame:
    if flow_runs.empty:
        return pd.DataFrame()

    by_tenant = build_workspace_by_tenant(api_url, token)
    failure_messages = fetch_failed_task_messages(flow_runs, logger)
    goal_cache: dict[tuple[str, str], dict | None] = {}
    causal_cache: dict[tuple[str, str | None, tuple[str, ...]], pd.DataFrame] = {}
    rows: list[dict] = []

    for index, flow_row in flow_runs.iterrows():
        signal_id = flow_row.get("parameters.audience")
        workspace = resolve_workspace(flow_row.get("parameters.tenant"), by_tenant)
        workspace_name = workspace["name"] if workspace else None
        excluded_dates = resolve_excluded_dates(
            exclude_dates_lookup,
            workspace,
            flow_row.get("parameters.tenant"),
        )

        if customer_filter and workspace_name not in customer_filter:
            continue

        signal = None
        model = None
        goal = None
        databricks_counts = {
            "treatment.session_count": None,
            "conversion.match_count": None,
            "treatment_conv_count.total": None,
        }

        if workspace and signal_id and not (isinstance(signal_id, float) and pd.isna(signal_id)):
            workspace_id = workspace["id"]
            signal = fetch_signal(api_url, token, workspace_id, str(signal_id))
            model = fetch_model_for_signal(api_url, token, workspace_id, signal, str(signal_id))
            goal_id = (signal or {}).get("config", {}).get("goal") or (model or {}).get("goal")
            goal = fetch_goal(api_url, token, workspace_id, goal_id, goal_cache)
            treatments = get_signal_treatments((signal or {}).get("config") or {})
            conversion_events = normalize_conversion_events(
                (goal or {}).get("conversionEvents") if goal else []
            )
            cut_date = extract_date_from_path((model or {}).get("path")) or (
                (model or {}).get("created", "")[:10] if model else None
            )
            try:
                databricks_counts = compute_databricks_counts(
                    workspace_id,
                    treatments,
                    conversion_events,
                    cut_date,
                    excluded_dates,
                    causal_cache,
                    treatment_count_lookback_days=treatment_count_lookback_days,
                )
                logger.info(f"databricks_counts: {databricks_counts}")
            except Exception as exc:
                logger.warning(
                    "Databricks counts failed for signal %s (%s): %s",
                    signal_id,
                    workspace_name,
                    exc,
                )

        rows.append(
            build_result_row(
                flow_row=flow_row,
                workspace=workspace,
                signal=signal,
                model=model,
                goal=goal,
                failure_message=failure_messages.get(flow_row["id"]),
                excluded_dates=excluded_dates,
                databricks_counts=databricks_counts,
            )
        )
        logger.info(
            "[%s/%s] %s | signal=%s | treatment_conv_count=%s",
            index + 1,
            len(flow_runs),
            workspace_name or flow_row.get("parameters.tenant"),
            signal_id,
            databricks_counts.get("treatment_conv_count.total"),
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result

    return result.sort_values(
        ["workspace.name", "flow_run.start_time", "signal.name"],
        na_position="last",
    ).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Debug failed k8-retraining flows with signal, model, and Databricks counts."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Look back this many days for failed retraining runs (default: 7)",
    )
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to workspace.name values (default: all customers)",
    )
    parser.add_argument(
        "--treatment-count-lookback-days",
        type=int,
        default=None,
        help=(
            "Look back this many days for treatment session counts "
            "(default: latest model release date)"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV output path (default: {DEFAULT_OUTPUT})",
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
    load_dotenv(SCRIPT_DIR / ".env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logging.info("Logging to %s", log_path)

    api_url = os.environ["URL"].rstrip("/")
    token = os.environ["API_SERVICE_TOKEN"]
    customer_filter = normalize_customer_filter(args.customers)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=args.days)
    logging.info(
        "Querying failed %s runs from %s to %s",
        FLOW_NAME,
        start_time.isoformat(),
        end_time.isoformat(),
    )

    flow_runs = query_failed_retraining_runs(start_time, end_time, logging.getLogger())
    logging.info("Found %s failed retraining runs", len(flow_runs))

    if flow_runs.empty:
        logging.info("No failed retraining runs in the selected window.")
        return

    try:
        exclude_dates_df = load_analytics_exclude_dates(logging.getLogger())
        exclude_dates_lookup = build_exclude_dates_lookup(exclude_dates_df)
    except Exception as exc:
        logging.warning(
            "Could not load analytics_exclude_dates from Delta Sharing: %s",
            exc,
        )
        exclude_dates_lookup = {}

    table = build_report_table(
        flow_runs,
        api_url,
        token,
        customer_filter,
        exclude_dates_lookup,
        logging.getLogger(),
        treatment_count_lookback_days=args.treatment_count_lookback_days,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    table_for_csv = table.copy()
    for column in ("signal.treatments", "signal.goal.conversionEvents", "excluded_dates"):
        if column in table_for_csv.columns:
            table_for_csv[column] = table_for_csv[column].apply(json.dumps)
    table_for_csv.to_csv(args.output, index=False)

    logging.info("Saved %s rows to %s", len(table), args.output)
    if not table.empty:
        pd.set_option("display.max_colwidth", 80)
        pd.set_option("display.width", 240)
        print(table.to_string(index=False))


if __name__ == "__main__":
    main()
