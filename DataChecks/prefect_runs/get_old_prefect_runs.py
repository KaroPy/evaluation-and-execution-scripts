from datetime import datetime, timedelta
import pandas as pd
from src.utils.prefect_api import call_prefect_api, format_date_for_prefect_api
from src.utils.logging_definitions import get_logger
from src.utils.directory_handling import create_directory_if_not_exists
def main():
    path = "DataChecks/prefect_runs/"
    path_data = f"{path}data/"
    path_logs = f"{path}logs/"
    create_directory_if_not_exists(path_data)
    create_directory_if_not_exists(path_logs)
    flow_name = "k8-targeting"
    today = datetime.today()
    logger = get_logger(f"{path}{today}_delete_old_runs")
    print("Start")
    logger.info("Start")
    start_time = today- timedelta(days=0)
    end_time = today - timedelta(days=2)
    start_time = format_date_for_prefect_api(start_time)
    end_time = format_date_for_prefect_api(end_time)
    endpoint = "/flow_runs/filter"
    payload = {
        "deployments": {"name": {"like_": flow_name}},
        "sort": "START_TIME_ASC",
        "flow_runs": {
            "start_time": {"after_": start_time, "before_": end_time},
            "state": {"type": {"any_":["RUNNING"]}}},
    }
    logger.info(f"Delete data json: {payload}")
    response = call_prefect_api(
        endpoint=endpoint,
        json_data=payload,
    )
    logger.info(f"response = {len(response)}")
    data = pd.json_normalize(response)
    data.to_csv(f"{path_data}{today}_long_running.csv")
    for entry in response:
        flow_run_id =entry["id"]
        print(entry)
        print(entry['tags'][1])
        endpoint = f"/flow_runs/{flow_run_id}/set_state"
        set_state = {"state":{
          "type": "CANCELLING",
          "message": "User requested cancellation"}}
        response_cancelled = call_prefect_api(
            endpoint=endpoint,
            json_data=set_state,
        )
        logger.info(f"{entry['tags'][1]} - {entry['start_time']}: response_cancelled: {response_cancelled}")


main()