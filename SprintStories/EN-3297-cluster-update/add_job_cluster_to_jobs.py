"""
For jobs matching a name filter, replace existing-cluster references with a
job-compute cluster using a standardised config.

Steps per job:
  1. Find tasks that use existing_cluster_id (not already a job cluster).
  2. Look up the existing cluster to get node_type_id and account tag.
  3. Build a job-cluster definition using the standard config template.
  4. Update the job: add the new job_cluster entry, swap every affected task
     from existing_cluster_id -> job_cluster_key.
"""

import copy
import requests
from datetime import datetime
from dotenv import load_dotenv
from general_functions.constants import (
    return_databricks_url,
    return_databricks_api_token,
)
from general_functions.define_logging import define_logging

load_dotenv()

JOB_NAME_FILTER = "save_incidents"
DRY_RUN = False  # Set to False to actually apply the changes


def _headers() -> dict:
    return {"Authorization": f"Bearer {return_databricks_api_token()}"}


def _get(endpoint: str, params: dict = None) -> dict:
    url = f"{return_databricks_url()}{endpoint}"
    response = requests.get(url, headers=_headers(), params=params)
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")
    return response.json()


def _post(endpoint: str, body: dict) -> dict:
    url = f"{return_databricks_url()}{endpoint}"
    response = requests.post(url, headers=_headers(), json=body)
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")
    return response.json()


def list_jobs_by_name(name_filter: str) -> list[dict]:
    matching = []
    params = {"limit": 26}
    while True:
        response = _get("api/2.1/jobs/list", params)
        for job in response.get("jobs", []):
            if name_filter.lower() in job.get("settings", {}).get("name", "").lower():
                matching.append(job)
        if not response.get("has_more", False):
            break
        params["page_token"] = response["next_page_token"]
    return matching


def get_job(job_id: int) -> dict:
    return _get("api/2.1/jobs/get", {"job_id": job_id})


def get_cluster(cluster_id: str) -> dict | None:
    try:
        return _get("api/2.1/clusters/get", {"cluster_id": cluster_id})
    except Exception as e:
        if "does not exist" in str(e):
            return None
        raise


def build_job_cluster(job_name: str, node_type_id: str, account: str) -> dict:
    return {
        "job_cluster_key": job_name,
        "new_cluster": {
            "data_security_mode": "DATA_SECURITY_MODE_DEDICATED",
            "custom_tags": {
                "account": account,
                "cluster_function": "job_runs",
            },
            "kind": "CLASSIC_PREVIEW",
            "spark_conf": {
                "spark.databricks.service.server.enabled": "true",
                "spark.hadoop.fs.s3a.secret.key": "{{secrets/etl-secrets/aws_access_secret_karo}}",
                "spark.hadoop.fs.s3a.access.key": "{{secrets/etl-secrets/aws_access_karo}}",
                "spark.rpc.message.maxSize": "389",
                "spark.sql.parquet.compression.codec": "zstd",
            },
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                "AWS_ACCESS_KEY_ID": "{{secrets/etl-secrets/aws_access_karo}}",
                "AWS_SECRET_ACCESS_KEY": "{{secrets/etl-secrets/aws_access_secret_karo}}",
                "SERVICE_TOKEN": "{{secrets/etl-secrets/innkeepr-token}}",
            },
            "runtime_engine": "STANDARD",
            "spark_version": "17.3.x-scala2.13",
            "node_type_id": node_type_id,
            "driver_node_type_id": node_type_id,
            "enable_elastic_disk": True,
            "init_scripts": [
                {
                    "workspace": {
                        "destination": "/Users/karoline@innkeepr.ai/production/cluster_init.sh"
                    }
                }
            ],
            "is_single_node": False,
            "num_workers": 1,
        },
    }


def process_job(job: dict, logger) -> None:
    job_id = job["job_id"]
    full_job = get_job(job_id)
    settings = full_job.get("settings", {})
    job_name = settings.get("name", "")

    tasks = settings.get("tasks", [])
    existing_job_clusters = settings.get("job_clusters", [])
    existing_job_cluster_keys = {jc["job_cluster_key"] for jc in existing_job_clusters}

    tasks_to_migrate = [t for t in tasks if "existing_cluster_id" in t]

    if not tasks_to_migrate:
        logger.info(f"[{job_name}] All tasks already use job clusters — skipping")
        return

    logger.info(f"[{job_name}] {len(tasks_to_migrate)} task(s) use existing_cluster_id")

    # Collect unique cluster IDs referenced by the tasks
    cluster_infos: dict[str, dict] = {}
    for task in tasks_to_migrate:
        cid = task["existing_cluster_id"]
        if cid not in cluster_infos:
            info = get_cluster(cid)
            if info is None:
                logger.warning(
                    f"  cluster {cid} no longer exists — task '{task.get('task_key')}' will be skipped"
                )
                cluster_infos[cid] = None
            else:
                cluster_infos[cid] = info
                logger.info(
                    f"  cluster {cid}: node_type={info.get('node_type_id')} "
                    f"account={info.get('custom_tags', {}).get('account')}"
                )

    # Drop tasks whose cluster couldn't be resolved
    tasks_to_migrate = [
        t
        for t in tasks_to_migrate
        if cluster_infos.get(t["existing_cluster_id"]) is not None
    ]
    cluster_infos = {
        cid: info for cid, info in cluster_infos.items() if info is not None
    }

    if not tasks_to_migrate:
        logger.warning(f"[{job_name}] No resolvable clusters found — skipping job")
        return

    # Build new job_clusters entries (one per unique existing cluster, unless key already exists)
    new_job_clusters = copy.deepcopy(existing_job_clusters)
    cluster_id_to_key: dict[str, str] = {}

    for cid, info in cluster_infos.items():
        node_type = info.get("node_type_id", "")
        account = info.get("custom_tags", {}).get("account", "")
        # Use job_name as key; if multiple clusters, suffix with node_type to stay unique
        key = job_name if len(cluster_infos) == 1 else f"{job_name}_{node_type}"

        if key in existing_job_cluster_keys:
            logger.info(f"  job_cluster_key '{key}' already exists — reusing")
        else:
            new_job_clusters.append(build_job_cluster(key, node_type, account))
            logger.info(f"  will add job_cluster_key='{key}' node_type={node_type}")

        cluster_id_to_key[cid] = key

    # Rewrite tasks: swap existing_cluster_id -> job_cluster_key
    new_tasks = copy.deepcopy(tasks)
    for task in new_tasks:
        cid = task.pop("existing_cluster_id", None)
        if cid:
            task["job_cluster_key"] = cluster_id_to_key[cid]

    body = {
        "job_id": job_id,
        "new_settings": {
            "job_clusters": new_job_clusters,
            "tasks": new_tasks,
        },
    }
    if DRY_RUN:
        logger.info(f"[{job_name}] DRY RUN — no changes applied - {body}")
        return

    _post(
        "api/2.1/jobs/update",
        body,
    )
    logger.info(f"[{job_name}] Updated successfully with {body}")


if __name__ == "__main__":
    logger = define_logging(
        f"SprintStories/EN-3297-cluster-update/add_job_cluster_to_jobs_{JOB_NAME_FILTER}_{datetime.now()}"
    )

    logger.info(f"Searching for jobs matching '{JOB_NAME_FILTER}'...")
    jobs = list_jobs_by_name(JOB_NAME_FILTER)
    logger.info(f"Found {len(jobs)} matching job(s)")

    for job in jobs:
        process_job(job, logger)
