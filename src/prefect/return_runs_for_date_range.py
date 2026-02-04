from datetime import timedelta
import pandas as pd
from src.utils.prefect_api import call_prefect_api, format_date_for_prefect_api
from src.prefect.constants import ACCEPTED_FLOW_STATES


DATE_STEPS = {
    "etlFlow": 3,
    "k8-targeting": 1,
    "k8-retraining": 1,
    "googleConversionUpdate": 3,
    "metaConversionUpdate": 3,
    "updateConversionTable": 3,
}
DEFAULT_LIMIT = 100


def get_runs_for_date_range(start_time, end_time, flow_name, logger, state=None):
    logger.info(f"Querying Prefect flow: {flow_name}")
    flow_runs = query_flow_run(
        start_time=start_time, end_time=end_time, flow_name=flow_name, logger=logger
    )
    runs = pd.json_normalize(flow_runs)
    runs.to_csv("all_run_properties.csv")  # change production
    runs = runs[
        [
            "deployment_id",
            "id",
            "state_type",
            "start_time",
            "parameters.tenant",
            "total_run_time",
            "auto_scheduled",
            "name",
        ]
    ]
    runs["flow_name"] = flow_name
    logger.info(f"Total runs retrieved: {len(runs)}")
    runs.to_csv("test.csv")
    runs = runs[
        (
            pd.to_datetime(runs["start_time"], utc=True)
            <= pd.to_datetime(end_time, utc=True)
        )
        & (
            pd.to_datetime(runs["start_time"], utc=True)
            >= pd.to_datetime(start_time, utc=True)
        )
    ]
    logger.info(f"Total runs after filtering by start_time: {len(runs)}")
    logger.info(
        f"Runs date range: {runs['start_time'].min()} - {runs['start_time'].max()}"
    )
    logger.info(f"Querying Prefect flow finished: {flow_name}")
    logger.info("-" * 50)
    return runs


def query_flow_run(start_time, end_time, flow_name, logger, state=None):
    endpoint = "/flow_runs/filter"
    all_responses = []
    end_time = pd.to_datetime(end_time, utc=True)
    while True:
        query_end_time = start_time + timedelta(days=DATE_STEPS.get(flow_name, 1))
        start_time = format_date_for_prefect_api(start_time)
        query_end_time = format_date_for_prefect_api(query_end_time)
        logger.info(
            f"Querying from {start_time} to {query_end_time} for flow {flow_name}"
        )
        payload = {
            "deployments": {"name": {"like_": flow_name}},
            "sort": "START_TIME_ASC",
            "flow_runs": {
                "start_time": {"after_": start_time, "before_": query_end_time},
            },
        }
        if state:
            payload["flow_runs"]["state"] = {"type": {"any_": state}}
        else:
            payload["flow_runs"]["state"] = {"type": {"any_": ACCEPTED_FLOW_STATES}}
        response = call_prefect_api(
            endpoint=endpoint,
            json_data=payload,
        )
        start_time = pd.to_datetime(query_end_time, utc=True)
        logger.info(f"response = {len(response)}")
        all_responses.extend(response)
        logger.info(f"Total collected so far: {len(all_responses)}")
        if start_time >= end_time:
            break
    return all_responses
