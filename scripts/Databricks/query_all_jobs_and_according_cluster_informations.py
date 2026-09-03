"""
Query all Databricks jobs and the clusters attached to each job.

Jobs API 2.1 returns at most 100 jobs per request, so listing paginates with
page_token until has_more is false. expand_tasks is set so task and cluster
details are included. For each job, attached clusters are collected from:

  - tasks[].existing_cluster_id  (persistent cluster; full spec via clusters/get)
  - settings.existing_cluster_id (legacy single-task jobs)
  - settings.job_clusters[]      (job-scoped new_cluster spec)
  - tasks[].new_cluster          (inline new_cluster spec)

Writes job_id, job_name, cluster_id, cluster_name, cluster type, and all
cluster properties (including tags) to a CSV table.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from general_functions.databricks_connection import (  # noqa: E402
    DatabricksClient,
    query_databricks,
)
from general_functions.define_logging import define_logging  # noqa: E402

load_dotenv()

JOBS_LIST_LIMIT = 100  # Jobs API 2.1 max per page
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = SCRIPT_DIR / "job_cluster_informations.csv"
LOG_PATH = SCRIPT_DIR / "query_all_jobs_and_according_cluster_informations"


def _json_or_value(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def list_all_jobs(client: DatabricksClient) -> list[dict]:
    """Return every job in the workspace, paging past the API limit."""
    endpoint = f"{client.databricks_url}api/2.1/jobs/list"
    jobs: list[dict] = []
    params: dict = {"limit": JOBS_LIST_LIMIT, "expand_tasks": "true"}

    while True:
        response = query_databricks(endpoint, client.headers, params=params)
        jobs.extend(response.get("jobs", []))
        if not response.get("has_more"):
            break
        page_token = response.get("next_page_token")
        if not page_token:
            break
        params["page_token"] = page_token

    return jobs


def get_cluster(client: DatabricksClient, cluster_id: str) -> dict | None:
    try:
        return client.return_custer_info(cluster_id)
    except Exception as exc:
        if "does not exist" in str(exc).lower() or "INVALID_PARAMETER_VALUE" in str(exc):
            return None
        raise


def flatten_cluster_properties(cluster: dict) -> dict:
    """Turn a cluster spec into table columns; tags become tag_<key> columns."""
    if not cluster:
        return {}

    row = {}
    custom_tags = cluster.get("custom_tags") or {}
    default_tags = cluster.get("default_tags") or {}

    for key, value in cluster.items():
        if key in ("custom_tags", "default_tags"):
            continue
        row[key] = _json_or_value(value)

    row["custom_tags"] = json.dumps(custom_tags, default=str) if custom_tags else None
    row["default_tags"] = json.dumps(default_tags, default=str) if default_tags else None

    for tag_key, tag_value in custom_tags.items():
        row[f"tag_{tag_key}"] = tag_value
    for tag_key, tag_value in default_tags.items():
        row[f"default_tag_{tag_key}"] = tag_value

    return row


def _collect_existing_cluster_ids(settings: dict) -> set[str]:
    ids: set[str] = set()
    existing = settings.get("existing_cluster_id")
    if existing:
        ids.add(existing)
    for task in settings.get("tasks", []):
        cid = task.get("existing_cluster_id")
        if cid:
            ids.add(cid)
    return ids


def _job_cluster_specs(settings: dict) -> list[tuple[str, dict]]:
    specs: list[tuple[str, dict]] = []
    for job_cluster in settings.get("job_clusters", []):
        key = job_cluster.get("job_cluster_key", "")
        specs.append((key, job_cluster.get("new_cluster") or {}))
    return specs


def _inline_new_clusters(settings: dict) -> list[dict]:
    clusters = []
    if settings.get("new_cluster"):
        clusters.append(settings["new_cluster"])
    for task in settings.get("tasks", []):
        if task.get("new_cluster"):
            clusters.append(task["new_cluster"])
    return clusters


def build_rows(client: DatabricksClient, jobs: list[dict], logger) -> list[dict]:
    cluster_cache: dict[str, dict | None] = {}
    rows: list[dict] = []

    for job in jobs:
        job_id = job.get("job_id")
        settings = job.get("settings") or {}
        job_name = settings.get("name", "")
        base = {"job_id": job_id, "job_name": job_name}

        attached = False

        for cluster_id in _collect_existing_cluster_ids(settings):
            attached = True
            if cluster_id not in cluster_cache:
                logger.info(f"Fetching cluster {cluster_id} for job {job_name}")
                cluster_cache[cluster_id] = get_cluster(client, cluster_id)
            cluster = cluster_cache[cluster_id] or {"cluster_id": cluster_id}
            row = {
                **base,
                "cluster_type": "existing_cluster",
                "job_cluster_key": None,
                "cluster_id": cluster.get("cluster_id", cluster_id),
                "cluster_name": cluster.get("cluster_name"),
            }
            row.update(flatten_cluster_properties(cluster))
            rows.append(row)

        for key, spec in _job_cluster_specs(settings):
            attached = True
            row = {
                **base,
                "cluster_type": "job_cluster",
                "job_cluster_key": key,
                "cluster_id": spec.get("cluster_id"),
                "cluster_name": spec.get("cluster_name") or key,
            }
            row.update(flatten_cluster_properties(spec))
            rows.append(row)

        for spec in _inline_new_clusters(settings):
            attached = True
            row = {
                **base,
                "cluster_type": "new_cluster_inline",
                "job_cluster_key": None,
                "cluster_id": spec.get("cluster_id"),
                "cluster_name": spec.get("cluster_name"),
            }
            row.update(flatten_cluster_properties(spec))
            rows.append(row)

        if not attached:
            rows.append(
                {
                    **base,
                    "cluster_type": "none",
                    "job_cluster_key": None,
                    "cluster_id": None,
                    "cluster_name": None,
                }
            )

    return rows


def main() -> None:
    logger = define_logging(str(LOG_PATH))
    client = DatabricksClient()

    logger.info("Listing all jobs (paginated, limit=%s)...", JOBS_LIST_LIMIT)
    jobs = list_all_jobs(client)
    logger.info("Found %s job(s)", len(jobs))

    logger.info("Resolving attached clusters...")
    rows = build_rows(client, jobs, logger)
    df = pd.DataFrame(rows)

    leading = [
        "job_id",
        "job_name",
        "cluster_id",
        "cluster_name",
        "cluster_type",
        "job_cluster_key",
        "custom_tags",
    ]
    ordered = [c for c in leading if c in df.columns] + [
        c for c in df.columns if c not in leading
    ]
    df = df[ordered]

    logger.info("\n%s", df.to_string(index=False))
    df.to_csv(OUTPUT_PATH, index=False)
    logger.info("Wrote %s row(s) to %s", len(df), OUTPUT_PATH)


if __name__ == "__main__":
    main()
