"""
Query long-running targeting flows from Prefect.

This module provides functionality to connect to Prefect and query targeting runs
that exceed a specified duration threshold.
"""

from asyncio import to_thread
import enum
from tracemalloc import start
import pandas as pd
from typing import Optional
from src.utils.prefect_api import PrefectFlowViaApi, call_prefect_api
from src.configs.prefect_specs import TARGETING_DEPLOYMENT
from general_functions.define_logging import define_logging


def get_long_targeting_runs(
    from_date: str,
    to_date: str,
    duration_threshold_seconds: int = 3600,
    logger=None,
    flow_name="k8-targeting",
) -> pd.DataFrame:
    """
    Query Prefect for targeting flow runs that exceeded a duration threshold.

    Args:
        from_date: Start date for the query in format 'YYYY-MM-DD'
        to_date: End date for the query in format 'YYYY-MM-DD'
        duration_threshold_seconds: Minimum duration in seconds to consider a run as "long"
                                   Default is 3600 seconds (1 hour)
        logger: Optional logger instance for logging

    Returns:
        pd.DataFrame: DataFrame containing long-running targeting flows with columns:
            - id: Flow run ID
            - name: Deployment name
            - deployment_id: Deployment ID
            - deployment_string: Parsed deployment type (e.g., 'k8-targeting')
            - account: Account name
            - audience_id: Audience ID
            - timestamp: Flow run start time
            - duration: Total run time in seconds
            - date: Date of the run
    """
    # Initialize Prefect API client
    if logger:
        logger.info(
            f"Querying long targeting runs from {from_date} to {to_date} "
            f"with duration threshold {duration_threshold_seconds}s"
        )

    prefect_client = PrefectFlowViaApi(logger=logger)

    # Extract all flows for the time range
    flows = prefect_client.extract_flows(
        from_date=from_date, to_date=to_date, save_data=False
    )

    if flows.empty:
        if logger:
            logger.warning("No flow runs found for the specified time range")
        return pd.DataFrame()

    # Filter for targeting deployments
    targeting_flows = flows[
        flows["deployment_string"].str.contains(flow_name, case=False, na=False)
    ]

    if targeting_flows.empty:
        if logger:
            logger.warning("No targeting flow runs found for the specified time range")
        return pd.DataFrame()

    # Getting task runs
    flow_run_ids = targeting_flows["id"].tolist()
    logger.info(f"flow_run_ids = {len(flow_run_ids)}")
    task_runs = pd.DataFrame()
    for start in range(0, len(flow_run_ids), 5):
        end = start + 5
        if end > len(flow_run_ids):
            end = len(flow_run_ids)
        logger.info(f". start = {start}, end = {end}")
        temp = prefect_client.extract_task_runs(flow_run_ids=flow_run_ids[start:end])
        logger.info(f"temp = {temp.shape}")
        # change production
        temp = temp[
            (temp["name"].str.contains("targeting_k8s", case=False, na=False))
            & (temp["state_type"].str.contains("COMPLETED", case=False, na=False))
        ][["flow_run_id", "created", "name", "total_run_time"]]
        task_runs = pd.concat([task_runs, temp], ignore_index=True)
    logger.info(f"task_runs = {task_runs.shape}")

    targeting_flows["id"] = targeting_flows["id"].astype("string")
    task_runs["flow_run_id"] = task_runs["flow_run_id"].astype("string")
    targeting_flows = pd.merge(
        targeting_flows,
        task_runs,
        left_on="id",
        right_on="flow_run_id",
        how="outer",
        suffixes=("", "_task_runs"),
    )

    targeting_flows["total_run_time"] = targeting_flows["total_run_time"].astype(
        "float"
    )
    logger.info(f"total_run_time stats: {targeting_flows['total_run_time'].describe()}")

    # Filter for long-running flows (duration in seconds)
    if duration is None:
        long_runs = targeting_flows
    else:
        long_runs = targeting_flows[
            targeting_flows["total_run_time"].astype("float")
            >= duration_threshold_seconds
        ]

    if logger:
        logger.info(
            f"Found {len(long_runs)} long-running targeting flows out of "
            f"{len(targeting_flows)} total targeting runs"
        )
        if not long_runs.empty:
            logger.info(
                f"Duration range: {long_runs['total_run_time'].min():.0f}s - "
                f"{long_runs['total_run_time'].max():.0f}s "
                f"(avg: {long_runs['total_run_time'].mean():.0f}s)"
            )

    return long_runs


if __name__ == "__main__":
    logger = define_logging("get_long_targeting_runs")

    start_date = "2025-11-01"
    end_date = "2025-11-19"
    duration = None
    all_stats = pd.DataFrame()
    date_range = pd.date_range(start_date, end_date, freq="D").strftime("%Y-%m-%d")
    logger.info(f"date_range = {date_range[0]} - {date_range[-1]}")
    for idate, from_date in enumerate(date_range):
        if idate == len(date_range) - 1:
            to_date = from_date  # date_range[-1]
        else:
            to_date = from_date  # date_range[idate + 1]
        logger.info(f"Get data from {from_date} to {to_date}")

        stats = get_long_targeting_runs(
            from_date, to_date, logger=logger, duration_threshold_seconds=duration
        )
        all_stats = pd.concat([all_stats, stats], ignore_index=True)
        if not stats.empty:
            all_stats.to_csv(
                f"SprintStories/EN-2933-Fix-Loading-Lstm/data/temp/{start_date}_{end_date}_targeting_stats_duratiaon_{duration}.csv",
                index=False,
            )
        logger.info(f"#################################################")
    all_stats.to_csv(
        f"SprintStories/EN-2933-Fix-Loading-Lstm/data/temp/{start_date}_{end_date}_targeting_stats_duratiaon_{duration}.csv",
        index=False,
    )
