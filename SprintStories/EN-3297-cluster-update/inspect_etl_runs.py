"""
Fetch runs of esn-etl-flow and rosental-etl-flow from yesterday and today.
Writes one row per task with full job, task, and cluster configuration.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from general_functions.constants import return_databricks_url, return_databricks_api_token
from general_functions.define_logging import define_logging

load_dotenv()

JOB_NAME_FILTERS = ["esn-etl-flow", "rosental-etl-flow"]


def _headers() -> dict:
    return {"Authorization": f"Bearer {return_databricks_api_token()}"}


def _get(endpoint: str, params: dict = None) -> dict:
    url = f"{return_databricks_url()}{endpoint}"
    response = requests.get(url, headers=_headers(), params=params)
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")
    return response.json()


def get_all_jobs() -> list[tuple[int, str]]:
    """Return [(job_id, job_name)] for every job in the workspace."""
    all_jobs = []
    params = {"limit": 26}
    while True:
        response = _get("api/2.1/jobs/list", params)
        for job in response.get("jobs", []):
            all_jobs.append((job["job_id"], job.get("settings", {}).get("name", "")))
        if not response.get("has_more", False):
            break
        params["page_token"] = response["next_page_token"]
    return all_jobs


def get_job_ids_by_names(
    name_filters: list[str], all_jobs: list[tuple[int, str]]
) -> dict[int, str]:
    """Return {job_id: job_name} for jobs matching any of the name filters."""
    return {
        job_id: name
        for job_id, name in all_jobs
        if any(f.lower() in name.lower() for f in name_filters)
    }


def get_runs_for_job(job_id: int, start_time_ms: int) -> list[dict]:
    runs = []
    params = {"job_id": job_id, "limit": 26, "start_time_from": start_time_ms, "expand_tasks": True}
    while True:
        response = _get("api/2.1/jobs/runs/list", params)
        runs.extend(response.get("runs", []))
        page_token = response.get("next_page_token")
        if not page_token:
            break
        params["page_token"] = page_token
    return runs


def get_job_settings(job_id: int) -> dict:
    return _get("api/2.1/jobs/get", {"job_id": job_id}).get("settings", {})


def get_run_details(run_id: int, include_history: bool = False) -> dict:
    return _get("api/2.1/jobs/runs/get", {"run_id": run_id, "include_history": include_history})


def get_cluster(cluster_id: str, cache: dict) -> dict:
    if cluster_id in cache:
        return cache[cluster_id]
    try:
        info = _get("api/2.1/clusters/get", {"cluster_id": cluster_id})
    except Exception as e:
        info = {"error": str(e)}
    cache[cluster_id] = info
    return info


def flatten_cluster(prefix: str, cfg: dict) -> dict:
    """Flatten a cluster config dict with a key prefix."""
    return {
        f"{prefix}spark_version": cfg.get("spark_version"),
        f"{prefix}node_type_id": cfg.get("node_type_id"),
        f"{prefix}driver_node_type_id": cfg.get("driver_node_type_id"),
        f"{prefix}num_workers": cfg.get("num_workers"),
        f"{prefix}autoscale": cfg.get("autoscale"),
        f"{prefix}data_security_mode": cfg.get("data_security_mode"),
        f"{prefix}runtime_engine": cfg.get("runtime_engine"),
        f"{prefix}kind": cfg.get("kind"),
        f"{prefix}enable_elastic_disk": cfg.get("enable_elastic_disk"),
        f"{prefix}is_single_node": cfg.get("is_single_node"),
        f"{prefix}custom_tags": cfg.get("custom_tags"),
        f"{prefix}spark_conf": cfg.get("spark_conf"),
        f"{prefix}spark_env_vars": cfg.get("spark_env_vars"),
        f"{prefix}init_scripts": cfg.get("init_scripts"),
        f"{prefix}state": cfg.get("state"),
    }


def ts(ms) -> str | None:
    if ms:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return None


def build_records(
    job_id: int,
    job_name: str,
    run: dict,
    job_settings: dict,
    cluster_cache: dict,
    is_repair: bool = False,
    repair_id: int | None = None,
    repair_attempt: int | None = None,
) -> list[dict]:
    run_id = run.get("run_id")
    run_state = run.get("state", {})
    job_cluster_cfgs = {
        jc["job_cluster_key"]: jc.get("new_cluster", {})
        for jc in job_settings.get("job_clusters", [])
    }

    base = {
        "job_id": job_id,
        "job_name": job_name,
        "run_id": run_id,
        "run_page_url": run.get("run_page_url"),
        "run_type": run.get("run_type"),
        "trigger": run.get("trigger"),
        "creator_user_name": run.get("creator_user_name"),
        "run_attempt": run.get("attempt_number"),
        "run_start": ts(run.get("start_time")),
        "run_end": ts(run.get("end_time")),
        "run_life_cycle_state": run_state.get("life_cycle_state"),
        "run_result_state": run_state.get("result_state"),
        "run_state_message": run_state.get("state_message"),
        "run_execution_duration_s": (run.get("execution_duration") or 0) / 1000,
        "run_setup_duration_s": (run.get("setup_duration") or 0) / 1000,
        "run_cleanup_duration_s": (run.get("cleanup_duration") or 0) / 1000,
        "run_queue_duration_s": (run.get("queue_duration") or 0) / 1000,
        "is_repair": is_repair,
        "repair_id": repair_id,
        "repair_attempt": repair_attempt,
    }

    tasks = run.get("tasks", [])
    if not tasks:
        return [{**base, "task_key": None, "cluster_type": None}]

    records = []
    for task in tasks:
        task_state = task.get("state", {})
        row = {
            **base,
            "task_key": task.get("task_key"),
            "task_run_id": task.get("run_id"),
            "task_start": ts(task.get("start_time")),
            "task_end": ts(task.get("end_time")),
            "task_life_cycle_state": task_state.get("life_cycle_state"),
            "task_result_state": task_state.get("result_state"),
            "task_state_message": task_state.get("state_message"),
            "task_execution_duration_s": (task.get("execution_duration") or 0) / 1000,
            "task_setup_duration_s": (task.get("setup_duration") or 0) / 1000,
            "task_cleanup_duration_s": (task.get("cleanup_duration") or 0) / 1000,
            "task_queue_duration_s": (task.get("queue_duration") or 0) / 1000,
            "task_attempt_number": task.get("attempt_number"),
            "task_depends_on": [d.get("task_key") for d in task.get("depends_on", [])],
        }

        # Cluster instance (actual cluster used at runtime)
        ci = task.get("cluster_instance", {})
        row["cluster_instance_id"] = ci.get("cluster_id")
        row["spark_context_id"] = ci.get("spark_context_id")

        # Cluster configuration source
        if "existing_cluster_id" in task:
            cid = task["existing_cluster_id"]
            row["cluster_type"] = "existing_cluster"
            row["cluster_id_or_key"] = cid
            cluster_cfg = get_cluster(cid, cluster_cache)
        elif "job_cluster_key" in task:
            key = task["job_cluster_key"]
            row["cluster_type"] = "job_cluster"
            row["cluster_id_or_key"] = key
            cluster_cfg = job_cluster_cfgs.get(key, {})
        elif "new_cluster" in task:
            row["cluster_type"] = "new_cluster_inline"
            row["cluster_id_or_key"] = None
            cluster_cfg = task["new_cluster"]
        else:
            row["cluster_type"] = "none"
            row["cluster_id_or_key"] = None
            cluster_cfg = {}

        row.update(flatten_cluster("cluster_", cluster_cfg))
        records.append(row)

    return records


if __name__ == "__main__":
    logger = define_logging("inspect_etl_runs")

    # Start of yesterday midnight UTC
    now = datetime.now(tz=timezone.utc)
    yesterday_midnight = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(yesterday_midnight.timestamp() * 1000)
    logger.info(f"Fetching runs from {yesterday_midnight.strftime('%Y-%m-%d %H:%M:%S UTC')} to now")

    logger.info("Fetching all jobs from workspace...")
    all_jobs = get_all_jobs()
    logger.info(f"All jobs ({len(all_jobs)}):")
    for jid, jname in sorted(all_jobs, key=lambda x: x[1].lower()):
        logger.info(f"  [{jid}] {jname}")

    job_map = get_job_ids_by_names(JOB_NAME_FILTERS, all_jobs)
    logger.info(
        f"Matched {len(job_map)} job(s) for filters {JOB_NAME_FILTERS}: {list(job_map.values())}"
    )

    cluster_cache: dict[str, dict] = {}
    all_records = []

    for job_id, job_name in job_map.items():
        logger.info(f"Fetching runs for '{job_name}' (id={job_id})...")
        job_settings = get_job_settings(job_id)
        runs = get_runs_for_job(job_id, start_ms)
        logger.info(f"  {len(runs)} run(s) found")
        for run in runs:
            original_run_id = run.get("run_id")

            # Use runs/get for complete task payload (runs/list is condensed)
            full_run = get_run_details(original_run_id, include_history=True)
            all_records.extend(
                build_records(job_id, job_name, full_run, job_settings, cluster_cache)
            )

            # Repair runs (retries)
            repairs = full_run.get("repair_history", [])
            if repairs:
                logger.info(f"  run {original_run_id} has {len(repairs)} repair(s)")
            for attempt, repair in enumerate(repairs, start=1):
                repair_id = repair.get("id")
                repair_state = repair.get("state", {})
                repair_run = get_run_details(repair_id)
                if not repair_run.get("state"):
                    repair_run["state"] = repair_state
                all_records.extend(
                    build_records(
                        job_id, job_name, repair_run, job_settings, cluster_cache,
                        is_repair=True,
                        repair_id=repair_id,
                        repair_attempt=attempt,
                    )
                )

    df = pd.DataFrame(all_records)
    logger.info(f"\nTotal rows: {len(df)}")
    logger.info(f"\n{df.to_string(index=False)}")

    output_path = "SprintStories/EN-3297-cluster-update/etl_runs_inspection.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")
