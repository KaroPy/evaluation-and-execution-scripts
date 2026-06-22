"""
Query treatment details for active treatments with 0 conversion sessions.

Reads audience_model_treatments_causal_results.csv, collects treatments labeled
error in audience.treatments, queries the targeting API, and saves one row per
unique treatment with campaign/ad account metadata and all audience.ids that
use it.
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

from check_signal_configuration import query_all_pages
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR,
    AUDIENCE_MODEL_TREATMENTS_LOGS_DIR as LOGS_DIR,
    SCRIPT_DIR,
)

CAUSAL_RESULTS_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_causal_results.csv"
)
DEFAULT_OUTPUT_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "error_treatments.csv"
)
OUTPUT_COLUMNS = [
    "workspace",
    "treatment.id",
    "treatment.relates_to.campaign.name",
    "treatment.name",
    "treatment.properties.adAccountname",
    "treatment.source",
    "audience.ids",
]


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"query_error_treatments_{stamp}.log"


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


def parse_treatment_summary(value: object) -> dict:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    return json.loads(str(value))


def collect_error_treatments(df: pd.DataFrame) -> dict[tuple[str, str, str], set[str]]:
    error_treatments: dict[tuple[str, str, str], set[str]] = {}
    for _, row in df.iterrows():
        workspace_name = row["workspace.name"]
        workspace_id = row["workspace.id"]
        audience_id = row["audience.id"]
        treatment_summary = parse_treatment_summary(row.get("audience.treatments"))
        for treatment_id, details in treatment_summary.items():
            if details.get("label") != "error":
                continue
            key = (workspace_name, workspace_id, treatment_id)
            error_treatments.setdefault(key, set()).add(audience_id)
    return error_treatments


def fetch_treatment(
    api_url: str,
    token: str,
    workspace_id: str,
    treatment_id: str,
) -> dict | None:
    treatments = query_all_pages(
        f"{api_url}/api/treatments/query",
        token,
        workspace_id,
        {"id": treatment_id},
    )
    return treatments[0] if treatments else None


def build_treatment_row(
    workspace_name: str,
    treatment_id: str,
    audience_ids: set[str],
    treatment: dict | None,
) -> dict:
    relates_to = (treatment or {}).get("relates_to") or {}
    campaign = relates_to.get("campaign") or {}
    properties = (treatment or {}).get("properties") or {}
    return {
        "workspace": workspace_name,
        "treatment.id": treatment_id,
        "treatment.relates_to.campaign.name": campaign.get("name"),
        "treatment.name": (treatment or {}).get("name"),
        "treatment.properties.adAccountname": properties.get("adAccountName"),
        "treatment.source": (treatment or {}).get("source"),
        "audience.ids": sorted(audience_ids),
    }


def run(
    csv_path: Path,
    api_url: str,
    token: str,
) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Causal results file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    error_treatments = collect_error_treatments(df)
    logging.info(
        "Found %s unique error treatment(s) across %s audience row(s)",
        len(error_treatments),
        len(df),
    )
    if not error_treatments:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows: list[dict] = []
    for (workspace_name, workspace_id, treatment_id), audience_ids in sorted(
        error_treatments.items(),
        key=lambda item: (item[0][0], item[0][2]),
    ):
        logging.info(
            "Querying treatment %s in %s (%s audience(s))",
            treatment_id,
            workspace_name,
            len(audience_ids),
        )
        treatment = fetch_treatment(api_url, token, workspace_id, treatment_id)
        if treatment is None:
            logging.warning("Treatment %s not found in workspace %s", treatment_id, workspace_name)
        rows.append(
            build_treatment_row(workspace_name, treatment_id, audience_ids, treatment)
        )

    return pd.DataFrame(rows)[OUTPUT_COLUMNS]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query treatment details for error-labeled treatments in causal results."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=CAUSAL_RESULTS_PATH,
        help="Path to audience_model_treatments_causal_results.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output CSV path (default: audience_model_treatments/data/error_treatments.csv)",
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

    api_url = os.environ.get("TARGETING_URL", "").rstrip("/")
    token = os.environ.get("API_SERVICE_TOKEN", "")
    if not api_url or not token:
        raise SystemExit("TARGETING_URL and API_SERVICE_TOKEN must be set in .env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logging.info("Logging to %s", log_path)

    results = run(args.csv, api_url, token)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    results_for_csv = results.copy()
    results_for_csv["audience.ids"] = results_for_csv["audience.ids"].apply(json.dumps)
    results_for_csv.to_csv(args.output, index=False)
    logging.info("Saved %s error treatment row(s) to %s", len(results), args.output)


if __name__ == "__main__":
    main()
