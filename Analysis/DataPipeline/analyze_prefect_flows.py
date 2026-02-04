import ast
import sys
import pandas as pd
from datetime import date, datetime, timedelta

from src.utils.directory_handling import create_directory_if_not_exists
from src.utils.logging_definitions import get_logger
from src.prefect.analyze_flow_runs import analyze_flow_runs
from src.prefect.analyze_subflows_and_tasks import analyze_subflows_and_tasks


def main(flow_names=["etlFlow"], window_in_days=1):
    today = datetime.today().date() - timedelta(days=1)
    path = "Analysis/DataPipeline/"
    path_data = f"{path}data/{today}/"
    path_logs = f"{path}logs/"
    create_directory_if_not_exists(path_data)
    create_directory_if_not_exists(path_logs)

    logger = get_logger(f"{path_logs}{datetime.now()}_analyze_prefect_flows")
    logger.info(
        f"Querying Prefect flows: {flow_names} for the past {window_in_days} days"
    )
    logger.info(f"Today: {today}")
    end_time = today
    start_time = end_time - timedelta(days=window_in_days)
    flow_runs = analyze_flow_runs(
        path_data,
        today,
        window_in_days,
        flow_names,
        start_time,
        end_time,
        logger,
    )
    analyze_subflows_and_tasks(
        flow_runs,
        f"{path_data}{today}_prefect_runs_analysis_{window_in_days}",
        logger,
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python [script_name.py] '[list_of_flow_names]' window_in_days")
        print("Example: python [script_name].py '['" "etlFlow" "']' 1")
        sys.exit()

    main(flow_names=ast.literal_eval(sys.argv[1]), window_in_days=int(sys.argv[2]))

# python Analysis/DataPipeline/analyze_prefect_flows.py '["etlFlow","k8-targeting","k8-retraining","googleConversionUpdate","metaConversionUpdate","updateConversionTable"]' 1
