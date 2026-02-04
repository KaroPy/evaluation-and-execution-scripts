import pandas as pd
from src.prefect.return_runs_for_date_range import get_runs_for_date_range
from src.prefect.data_transformation import add_columns
from src.prefect.return_stats import count_failed_and_success


def analyze_flow_runs(
    path_data, today, window_in_days, flow_names, start_time, end_time, logger
):
    all_flow_runs = pd.DataFrame()
    try:
        all_flow_runs = pd.read_csv(
            f"{path_data}{today}_prefect_runs_analysis_{window_in_days}.csv"
        )
    except FileNotFoundError:
        for flow_name in flow_names:
            flow_runs = get_runs_for_date_range(
                start_time=start_time,
                end_time=end_time,
                flow_name=flow_name,
                logger=logger,
            )
            all_flow_runs = pd.concat([all_flow_runs, flow_runs], ignore_index=True)
            all_flow_runs.to_csv(
                f"{path_data}{today}_prefect_runs_analysis_{window_in_days}_{flow_name}.csv"
            )
        all_flow_runs = add_columns(all_flow_runs)
        all_flow_runs.to_csv(
            f"{path_data}{today}_prefect_runs_analysis_{window_in_days}.csv"
        )
    all_flow_runs = add_columns(all_flow_runs)
    all_flow_runs.to_csv(
        f"{path_data}{today}_prefect_runs_analysis_{window_in_days}.csv"
    )
    logger.info(f"Total flow runs analyzed: {len(all_flow_runs)}")
    logger.info(f"Column names: {all_flow_runs.columns.tolist()}")
    count_failed_and_success(
        all_flow_runs,
        f"{path_data}{today}_prefect_runs_analysis_{window_in_days}",
        logger,
    )
    return all_flow_runs
