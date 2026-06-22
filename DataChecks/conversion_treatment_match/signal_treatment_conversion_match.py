"""
Query treatment–conversion session counts for a signal via Databricks SQL.

Loads signal.config.treatments and goal conversion events from the targeting API,
then queries innkeepr_databricks.<workspace>.features_view_<outlook>_outlook_train.

Usage (from repo root):
    python DataChecks/conversion_treatment_match/signal_treatment_conversion_match.py \\
        --customer Rosental --signal-id 69245bba105ef186b0db413c --outlook 30

    python DataChecks/conversion_treatment_match/signal_treatment_conversion_match.py \\
        --workspace-id 63eba9a1bfc19074666d7856 --signal-id 69245bba105ef186b0db413c \\
        --outlook 365 --cut-date 2026-01-01 --output /tmp/match.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
EN3327_DIR = REPO_ROOT / "SprintStories/EN-3327-Signal-Configuration"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(EN3327_DIR))

from check_audience_model_treatments import (  # type: ignore[import-not-found]  # noqa: E402
    filter_causal_check_for_audience,
    normalize_conversion_events,
    normalize_cut_date,
    treatment_conv_counts_for_audience,
)
from src.databricks_sql_client import (
    DatabricksSQLClient,  # type: ignore[import-not-found]
)

from general_functions.call_api_with_account_id import call_api_with_accountId  # noqa: E402
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = SCRIPT_DIR / "signal_treatment_conversion_match.csv"
DATABRICKS_CATALOG = "innkeepr_databricks"
SUPPORTED_OUTLOOKS = (30, 365)
CREATED_DATE_FORMAT = "%Y-%m-%d"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Treatment–conversion match counts for a signal via Databricks SQL."
    )
    workspace_group = parser.add_mutually_exclusive_group(required=True)
    workspace_group.add_argument("--workspace-id", metavar="ID", help="Targeting workspace id")
    workspace_group.add_argument("--customer", metavar="NAME", help="Workspace name, e.g. Rosental")
    parser.add_argument("--signal-id", required=True, metavar="ID", help="Signal (audience) id")
    parser.add_argument(
        "--outlook",
        type=int,
        choices=SUPPORTED_OUTLOOKS,
        default=30,
        help="Outlook window for features table (default: 30)",
    )
    parser.add_argument(
        "--cut-date",
        metavar="DATE",
        help=f"Only include rows with created >= DATE ({CREATED_DATE_FORMAT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV output path (default: {DEFAULT_OUTPUT.name})",
    )
    return parser.parse_args()


def resolve_workspace_id(customer: str | None, workspace_id: str | None) -> tuple[str, str]:
    if workspace_id:
        workspaces = return_workspace_ids(tracking_started=False)
        workspace = next((item for item in workspaces if item["id"] == workspace_id), None)
        if workspace is None:
            raise ValueError(f"Workspace id not found: {workspace_id}")
        return workspace["id"], workspace["name"]

    workspaces = return_workspace_ids(tracking_started=False)
    matches = [item for item in workspaces if item["name"] == customer]
    if not matches:
        known = sorted(item["name"] for item in workspaces)
        raise ValueError(f"Customer {customer!r} not found. Known workspaces: {known}")
    return matches[0]["id"], matches[0]["name"]


def fetch_signal(api_url: str, workspace_id: str, signal_id: str) -> dict:
    signals = call_api_with_accountId(
        f"{api_url}api/signals/query",
        workspace_id,
        {"id": signal_id},
        logging.getLogger(__name__),
    )
    if not signals:
        raise ValueError(f"Signal {signal_id} not found in workspace {workspace_id}")
    return signals[0]


def fetch_goal(api_url: str, workspace_id: str, goal_id: str | None) -> dict | None:
    if not goal_id:
        return None
    goals = call_api_with_accountId(
        f"{api_url}api/goals/query",
        workspace_id,
        {"id": goal_id},
        logging.getLogger(__name__),
    )
    return goals[0] if goals else None


def get_signal_treatments(config: dict) -> list[str]:
    treatments = config.get("treatments").get("ids") or []
    return [str(item) for item in treatments if item]


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def features_table_for_outlook(workspace_id: str, outlook: int) -> str:
    return f"{DATABRICKS_CATALOG}.{workspace_id}.features_view_{outlook}_outlook_train"


def build_treatment_conversion_match_sql(
    workspace_id: str,
    outlook: int,
    treatments: list[str],
    cut_date: str | None = None,
) -> str:
    if not treatments:
        raise ValueError("Signal has no treatments in config.treatments")

    treatment_literals = ", ".join(sql_literal(treatment) for treatment in treatments)
    table_name = features_table_for_outlook(workspace_id, outlook)
    cut_date_sql = normalize_cut_date(cut_date)
    created_filter = f"\n  WHERE to_date(created) >= DATE '{cut_date_sql}'" if cut_date_sql else ""
    return f"""
WITH deduped AS (
  SELECT DISTINCT session, treatment, conv_name
  FROM {table_name}{created_filter}
)
SELECT treatment, conv_name, COUNT(DISTINCT session) AS session_count
FROM deduped
WHERE treatment IN ({treatment_literals})
GROUP BY treatment, conv_name
ORDER BY conv_name, treatment
""".strip()


def query_databricks_sql(sql_statement: str) -> pd.DataFrame:
    client = DatabricksSQLClient()
    with client.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
            return cursor.fetchall_arrow().to_pandas()


def build_report(
    raw_result: pd.DataFrame,
    treatments: list[str],
    conversion_events: list[str],
    workspace_name: str,
    workspace_id: str,
    signal: dict,
    outlook: int,
    cut_date: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    filtered = filter_causal_check_for_audience(
        raw_result,
        treatments,
        conversion_events or None,
    )
    if filtered.empty:
        detail = pd.DataFrame(columns=["treatment", "conv_name", "session_count"])
    else:
        detail = filtered.sort_values(["conv_name", "treatment"]).reset_index(drop=True)

    per_treatment = treatment_conv_counts_for_audience(
        raw_result,
        treatments,
        conversion_events or None,
    )
    summary_rows = [
        {
            "workspace.name": workspace_name,
            "workspace.id": workspace_id,
            "signal.id": signal.get("id"),
            "signal.name": signal.get("name"),
            "outlook": outlook,
            "cut_date": cut_date,
            "treatment": treatment_id,
            "conv_name": "(all goal events)",
            "session_count": count,
        }
        for treatment_id, count in per_treatment.items()
    ]
    summary = pd.DataFrame(summary_rows)

    detail = detail.assign(
        **{
            "workspace.name": workspace_name,
            "workspace.id": workspace_id,
            "signal.id": signal.get("id"),
            "signal.name": signal.get("name"),
            "outlook": outlook,
            "cut_date": cut_date,
        }
    )
    detail = detail[
        [
            "workspace.name",
            "workspace.id",
            "signal.id",
            "signal.name",
            "outlook",
            "cut_date",
            "treatment",
            "conv_name",
            "session_count",
        ]
    ]
    return detail, summary


def main() -> None:
    args = parse_args()
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv(EN3327_DIR / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    api_url = return_api_url()
    workspace_id, workspace_name = resolve_workspace_id(args.customer, args.workspace_id)
    signal = fetch_signal(api_url, workspace_id, args.signal_id)
    config = signal.get("config") or {}
    treatments = get_signal_treatments(config)
    if not treatments:
        raise ValueError(f"Signal {args.signal_id} has no config.treatments")

    goal_id = signal.get("goal")
    goal = fetch_goal(api_url, workspace_id, goal_id)
    conversion_events = normalize_conversion_events(
        (goal or {}).get("conversionEvents") if goal else []
    )

    logger.info(
        "Signal %s (%s) | workspace %s | outlook %s | treatments=%s | conversionEvents=%s",
        signal.get("name"),
        args.signal_id,
        workspace_name,
        args.outlook,
        len(treatments),
        conversion_events,
    )

    sql_statement = build_treatment_conversion_match_sql(
        workspace_id,
        args.outlook,
        treatments,
        cut_date=args.cut_date,
    )
    logger.info("Querying %s", features_table_for_outlook(workspace_id, args.outlook))
    raw_result = query_databricks_sql(sql_statement)
    logger.info("Databricks returned %s treatment/conv_name rows", len(raw_result))

    detail, summary = build_report(
        raw_result,
        treatments,
        conversion_events,
        workspace_name,
        workspace_id,
        signal,
        args.outlook,
        args.cut_date,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    detail.to_csv(args.output, index=False)
    summary_path = args.output.with_name(f"{args.output.stem}_summary{args.output.suffix}")
    summary.to_csv(summary_path, index=False)

    total_match_sessions = int(summary["session_count"].sum()) if not summary.empty else 0
    logger.info(
        "Total conversion-match sessions (per treatment, deduped in SQL): %s", total_match_sessions
    )
    logger.info("Saved detail to %s", args.output)
    logger.info("Saved per-treatment summary to %s", summary_path)

    if detail.empty:
        print("No treatment/conversion matches found.")
        return

    pd.set_option("display.max_rows", 200)
    pd.set_option("display.width", 240)
    print(detail.to_string(index=False))
    print()
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
