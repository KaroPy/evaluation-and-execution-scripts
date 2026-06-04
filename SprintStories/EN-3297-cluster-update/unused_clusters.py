"""
List all clusters that were not used by any job run in the past 24 hours.

Steps:
  1. Fetch all clusters via api/2.1/clusters/list.
  2. Fetch all job runs from the last 24 h via api/2.1/jobs/runs/list.
  3. Collect the actual cluster IDs from cluster_instance fields in runs/tasks.
  4. Report clusters not present in that set.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

from general_functions.constants import return_databricks_api_token, return_databricks_url
from general_functions.define_logging import define_logging

load_dotenv()

HOURS = 16


def _headers() -> dict:
    return {"Authorization": f"Bearer {return_databricks_api_token()}"}


def _get(endpoint: str, params: dict = None) -> dict:
    url = f"{return_databricks_url()}{endpoint}"
    response = requests.get(url, headers=_headers(), params=params)
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")
    return response.json()


def list_all_clusters() -> list[dict]:
    return _get("api/2.1/clusters/list").get("clusters", [])


def _collect_cluster_ids_from_runs(params: dict) -> set[str]:
    used = set()
    while True:
        response = _get("api/2.1/jobs/runs/list", params)
        for run in response.get("runs", []):
            cid = run.get("cluster_instance", {}).get("cluster_id")
            if cid:
                used.add(cid)
            for task in run.get("tasks", []):
                cid = task.get("cluster_instance", {}).get("cluster_id")
                if cid:
                    used.add(cid)
                cid = task.get("existing_cluster_id")
                if cid:
                    used.add(cid)
        page_token = response.get("next_page_token")
        if not page_token:
            break
        params["page_token"] = page_token
    return used


def get_cluster_ids_used_in_runs(hours: int) -> set[str]:
    start_time_ms = int((datetime.now(tz=timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)

    # Runs that started within the window
    recently_started = _collect_cluster_ids_from_runs(
        {"limit": 26, "start_time_from": start_time_ms, "expand_tasks": True}
    )

    # Runs still active now (may have started before the window)
    currently_active = _collect_cluster_ids_from_runs(
        {"limit": 26, "active_only": True, "expand_tasks": True}
    )

    return recently_started | currently_active


if __name__ == "__main__":
    logger = define_logging("unused_clusters")

    logger.info("Fetching all clusters...")
    clusters = list_all_clusters()
    logger.info(f"Found {len(clusters)} cluster(s)")
    logger.info(f"Clusters: {clusters}")

    logger.info(f"Fetching cluster IDs used in job runs in the last {HOURS} hours...")
    used_ids = get_cluster_ids_used_in_runs(HOURS)
    logger.info(f"Found {len(used_ids)} unique cluster(s) used in recent runs")

    unused = [
        c for c in clusters
        if c.get("cluster_id") not in used_ids and c.get("state") != "RUNNING"
    ]
    logger.info(f"{len(unused)} cluster(s) were NOT used in the last {HOURS} hours:\n")

    records = [
        {
            "cluster_id": c.get("cluster_id"),
            "cluster_name": c.get("cluster_name"),
            "state": c.get("state"),
            "node_type_id": c.get("node_type_id"),
            "creator": c.get("creator_user_name"),
        }
        for c in unused
    ]

    df = pd.DataFrame(records)
    logger.info(f"\n{df.to_string(index=False)}")

    output_path = "SprintStories/EN-3297-cluster-update/unused_clusters.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Saved to {output_path}")
