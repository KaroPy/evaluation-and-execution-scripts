"""
Script to query Databricks jobs from the last X days including task states.

Uses the Databricks Jobs API 2.1 to list job runs and their task details.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Databricks configuration
DATABRICKS_URL = "https://dbc-4ed91336-7d96.cloud.databricks.com/"


def get_databricks_token() -> str:
    """Get Databricks API token from environment."""
    return os.environ["DATABRICKS_API_TOKEN"]


def query_databricks_api(
    endpoint: str, params: Optional[dict] = None, method: str = "GET"
) -> dict:
    """
    Query the Databricks API with authentication.

    Parameters
    ----------
    endpoint : str
        API endpoint path (e.g., 'api/2.1/jobs/runs/list')
    params : dict, optional
        Query parameters or request body
    method : str
        HTTP method (GET or POST)

    Returns
    -------
    dict
        JSON response from the API
    """
    url = f"{DATABRICKS_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {get_databricks_token()}"}

    if method == "GET":
        response = requests.get(url, headers=headers, params=params)
    else:
        response = requests.post(url, headers=headers, json=params)

    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code} - {response.text}")

    return response.json()


def get_job_runs(
    days: int = 7,
    job_id: Optional[int] = None,
    active_only: bool = False,
    completed_only: bool = False,
) -> list[dict]:  # USED
    """
    Get job runs from the last X days.

    Parameters
    ----------
    days : int
        Number of days to look back
    job_id : int, optional
        Filter by specific job ID
    active_only : bool
        Only return active/running jobs
    completed_only : bool
        Only return completed jobs

    Returns
    -------
    list[dict]
        List of job run dictionaries
    """
    start_time_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    all_runs = []
    page_token = None
    limit = 26

    while True:
        params = {
            "limit": limit,
            "start_time_from": start_time_ms,
            "expand_tasks": True,  # Include task details in response
        }

        # Use page_token for pagination (offset has 10k limit)
        if page_token:
            params["page_token"] = page_token

        if job_id:
            params["job_id"] = job_id
        if active_only:
            params["active_only"] = True
        if completed_only:
            params["completed_only"] = True

        response = query_databricks_api("api/2.1/jobs/runs/list", params)

        runs = response.get("runs", [])
        all_runs.extend(runs)

        # Check for next page token
        page_token = response.get("next_page_token")
        if not page_token:
            break

        print(f"Fetched {len(all_runs)} runs so far...")

    return all_runs


def get_run_details(run_id: int, include_history: bool = True) -> dict:
    """
    Get detailed information about a specific job run including tasks.

    Parameters
    ----------
    run_id : int
        The run ID to query
    include_history : bool
        Include repair history for the run (default: True)

    Returns
    -------
    dict
        Detailed run information including task states
    """
    params = {"run_id": run_id, "include_history": include_history}
    return query_databricks_api("api/2.1/jobs/runs/get", params)


def get_task_output(run_id: int) -> dict:
    """
    Get the output of a specific task run including logs and error traces.

    Parameters
    ----------
    run_id : int
        The task run ID to query

    Returns
    -------
    dict
        Task output including error, logs, and metadata
    """
    params = {"run_id": run_id}
    try:
        return query_databricks_api("api/2.1/jobs/runs/get-output", params)
    except Exception:
        return {}


def get_repair_history(run_id: int) -> list[dict]:
    """
    Get repair/retry history for a job run.

    Parameters
    ----------
    run_id : int
        The original run ID

    Returns
    -------
    list[dict]
        List of repair run attempts
    """
    try:
        run_details = get_run_details(run_id, include_history=True)
        return run_details.get("repair_history", [])
    except Exception:
        return []


def get_repair_runs_state(run_id: int) -> tuple[str, list[dict]]:
    """
    Get the aggregated state of all repair runs for a job run.

    If any repair run has FAILED state, returns FAILED.
    If all repair runs succeeded, returns SUCCESS.

    Parameters
    ----------
    run_id : int
        The original run ID

    Returns
    -------
    tuple[str, list[dict]]
        (aggregated_state, list of repair run details)
        aggregated_state is one of: FAILED, SUCCESS, NO_REPAIRS, or the last repair state
    """
    try:
        run_details = get_run_details(run_id, include_history=True)
        repair_history = run_details.get("repair_history", [])

        if not repair_history:
            return "NO_REPAIRS", []

        repair_states = []
        has_failure = False

        for repair in repair_history:
            repair_state = repair.get("state", {})
            result_state = repair_state.get("result_state", "")
            repair_states.append(
                {
                    "repair_id": repair.get("id"),
                    "result_state": result_state,
                    "life_cycle_state": repair_state.get("life_cycle_state", ""),
                    "start_time": repair.get("start_time"),
                    "end_time": repair.get("end_time"),
                }
            )
            if result_state == "FAILED":
                has_failure = True

        if has_failure:
            return "FAILED", repair_states

        # Return the last repair run's state if no failures
        last_state = repair_states[-1]["result_state"] if repair_states else "UNKNOWN"
        return last_state, repair_states

    except Exception:
        return "UNKNOWN", []


def extract_task_states(
    run: dict, include_output: bool = False, include_trial_runs: bool = False
) -> list[dict]:
    """
    Extract task states from a job run.

    Parameters
    ----------
    run : dict
        Job run dictionary from the API
    include_output : bool
        Fetch and include task output/error messages (default: False)
    include_trial_runs : bool
        Include trial/retry run information (default: False)

    Returns
    -------
    list[dict]
        List of task state dictionaries
    """
    tasks = []
    print(f"Run: ")
    print(run)
    run_id = run.get("run_id")
    job_id = run.get("job_id")
    run_name = run.get("run_name", "")
    state = run.get("status", {}).get("state", "")
    print(run_id, job_id, run_name, state)

    # Get run-level info
    run_state = run.get("state", {})
    run_life_cycle_state = run_state.get("life_cycle_state", "")
    run_result_state = run_state.get("result_state", "")
    print(
        f"Run life cycle state: {run_life_cycle_state}, result state: {run_result_state}, state: {run_state}"
    )

    # Get repair history (retries at the job level)
    run_infos = get_run_details(run_id)
    repair_history = run.get("repair_history", [])
    num_repairs = len(repair_history)
    if run.get("run_name") == "monitoring_update" and run_life_cycle_state != "RUNNING":
        response = get_repair_history(run_id)
        print("Repair history:")
        print(response)
        print("----")

        import sys

        sys.exit()

    # Calculate repair run state - FAILED if any repair failed
    repair_run_state = "NO_REPAIRS"
    if repair_history:
        has_repair_failure = False
        last_repair_state = None
        for repair in repair_history:
            repair_state = repair.get("state", {})
            last_repair_state = repair_state.get("result_state", "")
            if last_repair_state == "FAILED":
                has_repair_failure = True
        if has_repair_failure:
            repair_run_state = "FAILED"
        elif last_repair_state:
            repair_run_state = last_repair_state

    # Process tasks if available
    task_list = run.get("tasks", [])

    if not task_list:
        # If no tasks, create a single entry for the run itself
        task_record = {
            "run_id": run_id,
            "job_id": job_id,
            "run_name": run_name,
            "task_key": "main",
            "task_run_id": run_id,
            "life_cycle_state": run_life_cycle_state,
            "result_state": run_result_state,
            "repair_run_state": repair_run_state,
            "state_message": run_state.get("state_message", ""),
            "start_time": run.get("start_time"),
            "end_time": run.get("end_time"),
            "execution_duration_ms": run.get("execution_duration"),
            "setup_duration_ms": run.get("setup_duration"),
            "cleanup_duration_ms": run.get("cleanup_duration"),
            "attempt_number": run.get("attempt_number", 0),
            "num_repairs": num_repairs,
            "is_trial_run": False,
            "original_run_id": None,
            "error": None,
            "error_trace": None,
            "logs": None,
            "notebook_output": None,
        }

        if include_output:
            output = get_task_output(run_id)
            task_record["error"] = output.get("error")
            task_record["error_trace"] = output.get("error_trace")
            task_record["logs"] = output.get("logs")
            metadata = output.get("metadata", {})
            task_record["notebook_output"] = (
                metadata.get("result") if metadata else None
            )

        tasks.append(task_record)
    else:
        for task in task_list:
            task_state = task.get("state", {})
            task_run_id = task.get("run_id")

            task_record = {
                "run_id": run_id,
                "job_id": job_id,
                "run_name": run_name,
                "task_key": task.get("task_key", ""),
                "task_run_id": task_run_id,
                "life_cycle_state": task_state.get("life_cycle_state", ""),
                "result_state": task_state.get("result_state", ""),
                "repair_run_state": repair_run_state,
                "state_message": task_state.get("state_message", ""),
                "start_time": task.get("start_time"),
                "end_time": task.get("end_time"),
                "execution_duration_ms": task.get("execution_duration"),
                "setup_duration_ms": task.get("setup_duration"),
                "cleanup_duration_ms": task.get("cleanup_duration"),
                "attempt_number": task.get("attempt_number", 0),
                "depends_on": [
                    dep.get("task_key") for dep in task.get("depends_on", [])
                ],
                "cluster_instance": task.get("cluster_instance", {}).get("cluster_id"),
                "num_repairs": num_repairs,
                "is_trial_run": False,
                "original_run_id": None,
                "error": None,
                "error_trace": None,
                "logs": None,
                "notebook_output": None,
            }

            if include_output and task_run_id:
                output = get_task_output(task_run_id)
                task_record["error"] = output.get("error")
                task_record["error_trace"] = output.get("error_trace")
                task_record["logs"] = output.get("logs")
                metadata = output.get("metadata", {})
                task_record["notebook_output"] = (
                    metadata.get("result") if metadata else None
                )

            tasks.append(task_record)

    # Include trial/retry runs if requested
    if include_trial_runs and repair_history:
        for repair in repair_history:
            repair_state = repair.get("state", {})
            repair_task_run_ids = repair.get("task_run_ids", [])
            repair_id = repair.get("id")

            # Add a record for each repair attempt
            trial_record = {
                "run_id": run_id,
                "job_id": job_id,
                "run_name": run_name,
                "task_key": "repair_run",
                "task_run_id": repair_id,
                "life_cycle_state": repair_state.get("life_cycle_state", ""),
                "result_state": repair_state.get("result_state", ""),
                "state_message": repair_state.get("state_message", ""),
                "start_time": repair.get("start_time"),
                "end_time": repair.get("end_time"),
                "execution_duration_ms": None,
                "setup_duration_ms": None,
                "cleanup_duration_ms": None,
                "attempt_number": repair.get("repair_id", 0),
                "depends_on": [],
                "cluster_instance": None,
                "num_repairs": num_repairs,
                "is_trial_run": True,
                "original_run_id": run_id,
                "repair_task_run_ids": repair_task_run_ids,
                "error": None,
                "error_trace": None,
                "logs": None,
                "notebook_output": None,
            }

            if include_output and repair_id:
                output = get_task_output(repair_id)
                trial_record["error"] = output.get("error")
                trial_record["error_trace"] = output.get("error_trace")
                trial_record["logs"] = output.get("logs")

            tasks.append(trial_record)

    return tasks


def convert_timestamp(ts_ms: Optional[int]) -> Optional[datetime]:
    """Convert millisecond timestamp to datetime."""
    if ts_ms:
        return datetime.fromtimestamp(ts_ms / 1000)
    return None


def get_jobs_with_task_states(
    days: int = 7,
    job_id: Optional[int] = None,
    include_output: bool = False,
    include_trial_runs: bool = False,
    as_dataframe: bool = True,
) -> pd.DataFrame | list[dict]:
    """
    Get all job runs from the last X days with their task states.

    Parameters
    ----------
    days : int
        Number of days to look back (default: 7)
    job_id : int, optional
        Filter by specific job ID
    include_output : bool
        Fetch and include task output/error messages (default: False)
        Note: This makes additional API calls per task, which can be slow
    include_trial_runs : bool
        Include trial/retry run information (default: False)
    as_dataframe : bool
        Return as pandas DataFrame (default: True)

    Returns
    -------
    pd.DataFrame or list[dict]
        Job runs with task states
    """
    print(f"Fetching job runs from the last {days} days...")
    runs = get_job_runs(days=days, job_id=job_id)
    print(f"Found {len(runs)} job runs")

    if include_output:
        print("Fetching task outputs (this may take a while)...")

    all_tasks = []
    for i, run in enumerate(runs):
        if include_output and (i + 1) % 10 == 0:
            print(f"Processing run {i + 1}/{len(runs)}...")
        tasks = extract_task_states(
            run, include_output=include_output, include_trial_runs=include_trial_runs
        )
        all_tasks.extend(tasks)

    print(f"Extracted {len(all_tasks)} task states")

    if not as_dataframe:
        return all_tasks

    df = pd.DataFrame(all_tasks)

    if not df.empty:
        # Convert timestamps to datetime
        df["start_time"] = df["start_time"].apply(convert_timestamp)
        df["end_time"] = df["end_time"].apply(convert_timestamp)

        # Convert duration from ms to seconds
        for col in [
            "execution_duration_ms",
            "setup_duration_ms",
            "cleanup_duration_ms",
        ]:
            if col in df.columns:
                df[col.replace("_ms", "_sec")] = df[col] / 1000

        # Sort by start time descending
        df = df.sort_values("start_time", ascending=False)

    return df


def list_jobs() -> pd.DataFrame:
    """
    List all jobs in the workspace.

    Returns
    -------
    pd.DataFrame
        DataFrame with job information
    """
    all_jobs = []
    has_more = True
    offset = 0
    limit = 26

    while has_more:
        params = {"limit": limit, "offset": offset}
        response = query_databricks_api("api/2.1/jobs/list", params)

        jobs = response.get("jobs", [])
        all_jobs.extend(jobs)

        has_more = response.get("has_more", False)
        offset += limit

    if not all_jobs:
        return pd.DataFrame()

    # Extract relevant fields
    job_records = []
    for job in all_jobs:
        job_records.append(
            {
                "job_id": job.get("job_id"),
                "job_name": job.get("settings", {}).get("name", ""),
                "creator_user_name": job.get("creator_user_name", ""),
                "created_time": convert_timestamp(job.get("created_time")),
            }
        )

    return pd.DataFrame(job_records)


def get_run_output(run_id: int) -> dict:
    """
    Get the output of a specific run (useful for debugging failed runs).

    Parameters
    ----------
    run_id : int
        The run ID to query

    Returns
    -------
    dict
        Run output including error messages if any
    """
    params = {"run_id": run_id}
    return query_databricks_api("api/2.1/jobs/runs/get-output", params)


def summarize_task_states(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize task states by job and result state.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame from get_jobs_with_task_states

    Returns
    -------
    pd.DataFrame
        Summary of task states per job
    """
    if df.empty:
        return pd.DataFrame()

    summary = (
        df.groupby(["job_id", "run_name", "result_state"])
        .agg(
            task_count=("task_key", "count"),
            avg_execution_sec=("execution_duration_sec", "mean"),
            total_execution_sec=("execution_duration_sec", "sum"),
        )
        .reset_index()
    )

    return summary
