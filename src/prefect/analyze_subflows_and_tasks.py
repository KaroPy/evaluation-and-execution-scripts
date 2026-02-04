import pandas as pd
from src.utils.prefect_api import call_prefect_api
from src.prefect.return_stats import analyze_failed_flows, analyze_subflow_metrics


def analyze_subflows_and_tasks(df, path_data, logger, analyze_only_failed_taks=False):
    print(df.columns)
    path_data_subflow_tasks = f"{path_data}_subflows_and_tasks_failed.csv"
    try:
        logger.info("Trying to read existing subflows and tasks file")
        task_runs = pd.read_csv(path_data_subflow_tasks)
    except FileNotFoundError:
        logger.info("No existing subflows and tasks file found. Querying Prefect API.")
        all_responses = []
        for irow, row in df.iterrows():
            flow_run_id = row.id
            state = row.state_type
            deployment_id = row.deployment_id
            if state != "FAILED" and analyze_only_failed_taks:
                continue
            logger.info(
                f"{irow}/{len(df)}: Analyzing flow run: {flow_run_id} with state: {state}"
            )
            responses = query_flow_run_via_id(flow_run_id, deployment_id, logger)
            logger.info(f"Number of task runs found: {len(responses)}")
            all_responses.extend(responses)
        task_runs = pd.json_normalize(all_responses)
        task_runs = task_runs.rename(
            columns={
                "id": "task_run_id",
                "flow_run_id": "id",
                "name": "task_name",
                "state_type": "task_state_type",
                "total_run_time": "task_total_run_time",
            }
        )
        task_runs = task_runs[
            [
                "id",
                "task_run_id",
                "task_name",
                "task_state_type",
                "state.message",
                "task_total_run_time",
            ]
        ]
        pd.DataFrame(task_runs).to_csv(path_data_subflow_tasks, index=False)
    df = pd.merge(df, task_runs, on="id", how="left")
    df.to_csv(f"{path_data}_with_subflows_and_tasks.csv", index=False)
    analyze_failed_flows(
        df[df["task_state_type"] == "FAILED"],
        f"{path_data}_with_subflows_and_tasks",
        logger,
    )
    analyze_subflow_metrics(df, f"{path_data}_with_subflows_and_tasks", logger)
    return df


def query_flow_run_via_id(flow_run_id, deployment_id, logger):
    endpoint = f"/task_runs/filter"
    payload = {
        "flow_runs": {"id": {"any_": [flow_run_id]}},
        "deployments": {"id": {"any_": [deployment_id]}},
    }
    response = call_prefect_api(
        endpoint=endpoint,
        json_data=payload,
    )
    logger.info(f"Response length: {len(response)}")

    return response
