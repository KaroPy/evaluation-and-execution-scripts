"""
Trigger conversion model retraining for audiences with enough causal sessions.

Reads audience_model_treatments_causal_results.csv, selects rows where
treatment_conv_count.total meets the threshold, re-verifies via the targeting
API that the audience still uses a conversion model, then triggers the
existing per-audience k8-retraining Prefect deployment (by deployment id)
with reset_lstm=True and max_model_age_in_days=0. Waits 90 seconds between
consecutive triggers.

Dry-run is the default. Pass --apply to create flow runs on existing deployments.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from check_signal_configuration import api_post, fetch_model, sanitize_workspace_name
from src.paths import (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR,
    AUDIENCE_MODEL_TREATMENTS_LOGS_DIR as LOGS_DIR,
    SCRIPT_DIR,
)

CAUSAL_RESULTS_PATH = (
    AUDIENCE_MODEL_TREATMENTS_DATA_DIR / "audience_model_treatments_causal_results.csv"
)
RETRAINING_FLOW_NAME = "k8-retraining"
DEFAULT_MIN_SESSIONS = 500
DEFAULT_WAIT_SECONDS = 90
DEPLOYMENT_NAME_PATTERN = re.compile(
    r"^k8-retraining-(?P<tenant>.+)-(?P<audience_id>[0-9a-f]{24})-"
)


@dataclass(frozen=True)
class RetrainingDeployment:
    id: str
    full_name: str


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"trigger_conversion_retraining_{stamp}.log"


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

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


def load_eligible_rows(
    csv_path: Path,
    min_sessions: int,
    workspace_filter: set[str] | None,
) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Causal results file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if "treatment_conv_count.total" not in df.columns:
        raise ValueError(
            "Expected column treatment_conv_count.total in causal results CSV"
        )

    df["treatment_conv_count.total"] = pd.to_numeric(
        df["treatment_conv_count.total"], errors="coerce"
    ).fillna(0)
    eligible = df[df["treatment_conv_count.total"] >= min_sessions].copy()
    if workspace_filter:
        eligible = eligible[eligible["workspace.name"].isin(workspace_filter)]
    return eligible.sort_values(
        ["workspace.name", "audience.name"],
        na_position="last",
    ).reset_index(drop=True)


async def build_retraining_deployment_index() -> dict[tuple[str, str], RetrainingDeployment]:
    from prefect.client.orchestration import get_client

    index: dict[tuple[str, str], RetrainingDeployment] = {}
    async with get_client() as client:
        offset = 0
        while True:
            deployments = await client.read_deployments(limit=200, offset=offset)
            if not deployments:
                break
            for deployment in deployments:
                name = deployment.name or ""
                match = DEPLOYMENT_NAME_PATTERN.match(name)
                if not match:
                    continue
                key = (match.group("tenant"), match.group("audience_id"))
                index[key] = RetrainingDeployment(
                    id=str(deployment.id),
                    full_name=f"{RETRAINING_FLOW_NAME}/{name}",
                )
            if len(deployments) < 200:
                break
            offset += 200
    return index


def fetch_audience(
    api_url: str,
    token: str,
    workspace_id: str,
    audience_id: str,
) -> dict | None:
    audiences = api_post(
        f"{api_url}/api/audiences/query",
        token,
        {
            "content": {"id": audience_id},
            "context": {"workspaceId": workspace_id},
        },
    )
    return audiences[0] if audiences else None


def is_conversion_model(
    api_url: str,
    token: str,
    workspace_id: str,
    audience: dict,
) -> tuple[bool, str | None]:
    if audience.get("status") != "active":
        return False, f"audience status is {audience.get('status')!r}"

    config = audience.get("config", {})
    model_id = config.get("model")
    model = fetch_model(api_url, token, workspace_id, model_id)
    model_type = model.get("type")
    if model_type != "conversion":
        return False, f"model.type is {model_type!r}"
    return True, model_type


def resolve_deployment(
    deployment_index: dict[tuple[str, str], RetrainingDeployment],
    workspace_name: str,
    audience_id: str,
) -> RetrainingDeployment | None:
    tenant = sanitize_workspace_name(workspace_name)
    return deployment_index.get((tenant, audience_id))


def trigger_prefect_deployment(
    deployment: RetrainingDeployment,
    tenant: str,
    audience_id: str,
    dry_run: bool,
) -> str | None:
    params = {
        "tenant": tenant,
        "audience": audience_id,
        "reset_lstm": True,
        "max_model_age_in_days": 0,
    }
    if dry_run:
        logging.info(
            "DRY-RUN would trigger existing deployment id=%s name=%s with params %s",
            deployment.id,
            deployment.full_name,
            json.dumps(params, sort_keys=True),
        )
        return None

    cmd = ["prefect", "deployment", "run", "--id", deployment.id]
    for key, value in params.items():
        cmd.extend(["-p", f"{key}={json.dumps(value)}"])

    logging.info(
        "Triggering existing deployment id=%s name=%s",
        deployment.id,
        deployment.full_name,
    )
    logging.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"prefect deployment run failed for deployment id={deployment.id} "
            f"name={deployment.full_name}: {stderr}"
        )

    output = (result.stdout or "").strip()
    if output:
        for line in output.splitlines():
            logging.info(line)
    if result.stderr and result.stderr.strip():
        for line in result.stderr.strip().splitlines():
            logging.info(line)
    for line in output.splitlines():
        if "flow run" in line.lower() and "│" not in line:
            return line.strip()
    return output.splitlines()[-1].strip() if output else None


def run(
    csv_path: Path,
    min_sessions: int,
    wait_seconds: int,
    workspace_filter: set[str] | None,
    dry_run: bool,
    api_url: str,
    token: str,
) -> list[dict]:
    eligible = load_eligible_rows(csv_path, min_sessions, workspace_filter)
    logging.info(
        "Loaded %s eligible audience(s) with treatment_conv_count.total >= %s",
        len(eligible),
        min_sessions,
    )
    if eligible.empty:
        return []

    deployment_index = asyncio.run(build_retraining_deployment_index())
    logging.info("Indexed %s k8-retraining deployment(s)", len(deployment_index))

    results: list[dict] = []
    rows = list(eligible.iterrows())

    for row_index, (_, row) in enumerate(rows):
        workspace_name = row["workspace.name"]
        workspace_id = row["workspace.id"]
        audience_id = row["audience.id"]
        audience_name = row["audience.name"]
        total_sessions = int(row["treatment_conv_count.total"])
        tenant = sanitize_workspace_name(workspace_name)

        result = {
            "workspace.name": workspace_name,
            "workspace.id": workspace_id,
            "audience.id": audience_id,
            "audience.name": audience_name,
            "tenant": tenant,
            "treatment_conv_count.total": total_sessions,
            "status": "pending",
            "detail": "",
            "deployment.id": None,
            "deployment": None,
            "flow_run": None,
        }

        logging.info(
            "Checking %s / %s (%s sessions)",
            workspace_name,
            audience_name,
            total_sessions,
        )

        audience = fetch_audience(api_url, token, workspace_id, audience_id)
        if audience is None:
            result["status"] = "skipped"
            result["detail"] = "audience not found"
            logging.warning("Skipping %s: audience not found", audience_id)
            results.append(result)
            continue

        is_conversion, detail = is_conversion_model(
            api_url, token, workspace_id, audience
        )
        if not is_conversion:
            result["status"] = "skipped"
            result["detail"] = detail or "not a conversion model"
            logging.warning(
                "Skipping %s / %s: %s",
                workspace_name,
                audience_name,
                result["detail"],
            )
            results.append(result)
            continue

        deployment = resolve_deployment(
            deployment_index, workspace_name, audience_id
        )
        if deployment is None:
            result["status"] = "error"
            result["detail"] = (
                f"no existing k8-retraining deployment for tenant={tenant!r}, "
                f"audience={audience_id}"
            )
            logging.error("%s", result["detail"])
            results.append(result)
            continue

        result["deployment.id"] = deployment.id
        result["deployment"] = deployment.full_name
        logging.info(
            "Resolved existing deployment id=%s name=%s",
            deployment.id,
            deployment.full_name,
        )
        try:
            flow_run = trigger_prefect_deployment(
                deployment,
                tenant,
                audience_id,
                dry_run=dry_run,
            )
        except RuntimeError as exc:
            result["status"] = "error"
            result["detail"] = str(exc)
            logging.error("%s", exc)
            results.append(result)
            continue

        result["status"] = "dry-run" if dry_run else "triggered"
        result["flow_run"] = flow_run
        results.append(result)

        if not dry_run and result["status"] == "triggered" and row_index < len(rows) - 1:
            logging.info("Waiting %s seconds before next trigger", wait_seconds)
            time.sleep(wait_seconds)

    return results


def write_results(results: list[dict], output_path: Path, log_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)
    logging.info("Wrote results CSV to %s", output_path)

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("\n--- results ---\n")
        if results_df.empty:
            handle.write("(no results)\n")
        else:
            handle.write(results_df.to_csv(index=False))
        handle.write("--- end results ---\n")
    logging.info("Wrote results to log file %s", log_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Trigger conversion model retraining for audiences with enough "
            "causal conversion sessions."
        )
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=CAUSAL_RESULTS_PATH,
        help="Path to audience_model_treatments_causal_results.csv",
    )
    parser.add_argument(
        "--min-sessions",
        type=int,
        default=DEFAULT_MIN_SESSIONS,
        help="Minimum treatment_conv_count.total required (default: 500)",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=DEFAULT_WAIT_SECONDS,
        help="Seconds to wait between triggers (default: 90)",
    )
    parser.add_argument(
        "--customer",
        action="append",
        help="Only process these workspace.name values (repeatable, comma-separated)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Trigger Prefect deployments (default is dry-run)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Log file path (default: audience_model_treatments/logs/trigger_conversion_retraining_<timestamp>.log)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional CSV path for run results (default: same stem as log file in logs/)",
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

    workspace_filter = normalize_customer_filter(args.customer)
    dry_run = not args.apply
    if dry_run:
        logging.info("Dry-run mode (pass --apply to trigger Prefect flows)")

    results = run(
        csv_path=args.csv,
        min_sessions=args.min_sessions,
        wait_seconds=args.wait_seconds,
        workspace_filter=workspace_filter,
        dry_run=dry_run,
        api_url=api_url,
        token=token,
    )

    status_counts: dict[str, int] = {}
    for item in results:
        status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1

    logging.info("Summary: %s", status_counts)

    output_path = args.output or log_path.with_suffix(".csv")
    write_results(results, output_path, log_path)


if __name__ == "__main__":
    main()
