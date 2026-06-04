"""
Check that the account tag on each job's cluster matches the account extracted
from the job name (first segment when split by '_' or '-').

For each job:
  - Extract account = first part of job name split by '_' or '-', lowercased
  - Look up the account custom_tag from:
      * job_clusters[].new_cluster.custom_tags.account
      * tasks[].existing_cluster_id -> clusters/get -> custom_tags.account
  - Report rows where extracted account != tag account
"""

import re
import requests
import pandas as pd
from dotenv import load_dotenv
from general_functions.constants import return_databricks_url, return_databricks_api_token
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


def list_all_job_ids() -> list[int]:
    job_ids = []
    params = {"limit": 26}
    while True:
        response = _get("api/2.1/jobs/list", params)
        for job in response.get("jobs", []):
            job_ids.append(job["job_id"])
        if not response.get("has_more", False):
            break
        params["page_token"] = response["next_page_token"]
    return job_ids


def get_job(job_id: int) -> dict:
    return _get("api/2.1/jobs/get", {"job_id": job_id})


def get_cluster(cluster_id: str) -> dict | None:
    try:
        return _get("api/2.1/clusters/get", {"cluster_id": cluster_id})
    except Exception as e:
        if "does not exist" in str(e):
            return None
        raise


def sanitize(value: str) -> str:
    return (
        value.lower()
        .strip()
        .replace(" ", "")
        .replace("ö", "oe")
        .replace(".", "dot")
        .replace("-", "")
    )


def extract_account_from_name(job_name: str) -> str:
    return sanitize(re.split(r"[_\-]", job_name)[0])


def get_account_tags_for_job(settings: dict, cluster_cache: dict) -> list[str]:
    """Return all distinct account tag values found across job clusters and tasks."""
    accounts = set()

    # Job-level cluster definitions
    for jc in settings.get("job_clusters", []):
        tag = jc.get("new_cluster", {}).get("custom_tags", {}).get("account")
        if tag:
            accounts.add(sanitize(tag))

    # Task-level existing clusters
    for task in settings.get("tasks", []):
        cid = task.get("existing_cluster_id")
        if not cid:
            continue
        if cid not in cluster_cache:
            cluster_cache[cid] = get_cluster(cid)
        info = cluster_cache[cid]
        if info:
            tag = info.get("custom_tags", {}).get("account")
            if tag:
                accounts.add(sanitize(tag))

    return sorted(accounts)


if __name__ == "__main__":
    logger = define_logging("check_job_account_tags")

    logger.info("Fetching all job IDs...")
    job_ids = list_all_job_ids()
    logger.info(f"Found {len(job_ids)} jobs")

    cluster_cache: dict[str, dict] = {}
    records = []

    for job_id in job_ids:
        job = get_job(job_id)
        settings = job.get("settings", {})
        job_name = settings.get("name", "")

        extracted = extract_account_from_name(job_name)
        tag_accounts = get_account_tags_for_job(settings, cluster_cache)

        if not tag_accounts:
            records.append({
                "job_id": job_id,
                "job_name": job_name,
                "extracted_account": extracted,
                "tag_account": None,
                "match": False,
            })
        else:
            for tag_account in tag_accounts:
                records.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "extracted_account": extracted,
                    "tag_account": tag_account,
                    "match": extracted == tag_account,
                })

    df = pd.DataFrame(records)

    mismatches = df[~df["match"]]
    logger.info(f"\n{len(mismatches)} job(s) with mismatched account tag:\n{mismatches.to_string(index=False)}")

    output_path = "SprintStories/EN-3297-cluster-update/job_account_tag_check.csv"
    df.to_csv(output_path, index=False)
    logger.info(f"Full results saved to {output_path}")
