"""
Extract all Databricks jobs, their clusters, and cluster (DBR) versions.

For each job, inspects task-level cluster config (existing cluster, job cluster,
or inline new_cluster) and resolves the Databricks Runtime spark_version.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from general_functions.constants import (
    return_databricks_url,
    return_databricks_api_token,
)
from general_functions.define_logging import define_logging

load_dotenv()


def _headers() -> dict:
    return {"Authorization": f"Bearer {return_databricks_api_token()}"}


def _get(endpoint: str, params: dict = None) -> dict:
    url = f"{return_databricks_url()}{endpoint}"
    response = requests.get(url, headers=_headers(), params=params)
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")
    return response.json()


def get_job_ids_run_in_last_hours(hours: int = 48) -> list[int]:
    start_time_ms = int((datetime.now(tz=timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)
    job_ids = set()
    params = {"limit": 26, "start_time_from": start_time_ms}
    while True:
        response = _get("api/2.1/jobs/runs/list", params)
        for run in response.get("runs", []):
            if "job_id" in run:
                job_ids.add(run["job_id"])
        page_token = response.get("next_page_token")
        if not page_token:
            break
        params["page_token"] = page_token
    return list(job_ids)


def get_job(job_id: int) -> dict:
    return _get("api/2.1/jobs/get", {"job_id": job_id})


def get_cluster_version(cluster_id: str, cluster_cache: dict) -> str:
    if cluster_id in cluster_cache:
        return cluster_cache[cluster_id]
    try:
        info = _get("api/2.1/clusters/get", {"cluster_id": cluster_id})
        version = info.get("spark_version", "unknown")
    except Exception:
        version = "not_found"
    cluster_cache[cluster_id] = version
    return version


def extract_job_cluster_info(jobs: list[dict], logger) -> list[dict]:
    records = []
    cluster_cache = {}

    for job in jobs:
        job_id = job.get("job_id")
        job_name = job.get("settings", {}).get("name", "")

        # Build map of job_cluster_key -> spark_version from job-level cluster definitions
        job_cluster_versions = {}
        for jc in job.get("settings", {}).get("job_clusters", []):
            key = jc.get("job_cluster_key", "")
            version = jc.get("new_cluster", {}).get("spark_version", "unknown")
            job_cluster_versions[key] = version

        tasks = job.get("settings", {}).get("tasks", [])

        if not tasks:
            # Job with no tasks — record job-level cluster if any
            for key, version in job_cluster_versions.items():
                records.append(
                    {
                        "job_id": job_id,
                        "job_name": job_name,
                        "task_key": None,
                        "cluster_type": "job_cluster",
                        "cluster_id_or_key": key,
                        "spark_version": version,
                    }
                )
            if not job_cluster_versions:
                records.append(
                    {
                        "job_id": job_id,
                        "job_name": job_name,
                        "task_key": None,
                        "cluster_type": "none",
                        "cluster_id_or_key": None,
                        "spark_version": None,
                    }
                )
            continue

        for task in tasks:
            task_key = task.get("task_key", "")
            record = {
                "job_id": job_id,
                "job_name": job_name,
                "task_key": task_key,
                "cluster_type": None,
                "cluster_id_or_key": None,
                "spark_version": None,
            }

            if "existing_cluster_id" in task:
                cluster_id = task["existing_cluster_id"]
                logger.info(
                    f"  [{job_name}] task={task_key} existing_cluster={cluster_id}"
                )
                record["cluster_type"] = "existing_cluster"
                record["cluster_id_or_key"] = cluster_id
                record["spark_version"] = get_cluster_version(cluster_id, cluster_cache)

            elif "job_cluster_key" in task:
                key = task["job_cluster_key"]
                logger.info(f"  [{job_name}] task={task_key} job_cluster_key={key}")
                record["cluster_type"] = "job_cluster"
                record["cluster_id_or_key"] = key
                record["spark_version"] = job_cluster_versions.get(key, "unknown")

            elif "new_cluster" in task:
                version = task["new_cluster"].get("spark_version", "unknown")
                logger.info(
                    f"  [{job_name}] task={task_key} inline new_cluster version={version}"
                )
                record["cluster_type"] = "new_cluster_inline"
                record["cluster_id_or_key"] = None
                record["spark_version"] = version

            else:
                record["cluster_type"] = "none"

            records.append(record)

    return records


if __name__ == "__main__":
    logger = define_logging("extract_job_cluster_versions")

    logger.info("Fetching job IDs with runs in the last 48 hours...")
    job_ids = get_job_ids_run_in_last_hours(hours=48)
    logger.info(f"Found {len(job_ids)} jobs — fetching full details...")

    jobs = []
    for jid in job_ids:
        jobs.append(get_job(jid))

    logger.info("Resolving cluster versions per task...")
    records = extract_job_cluster_info(jobs, logger)

    df = pd.DataFrame(records)
    logger.info(f"\n{df.to_string(index=False)}")

    output_path = "SprintStories/EN-3297-cluster-update/job_cluster_versions.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")
