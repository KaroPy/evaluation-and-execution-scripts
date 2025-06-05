import pandas as pd
import numpy as np

from src.utils.accounts import sanitize_account_name
from src.configs.data_specs import col_date

DEPLOYMENTS_MATCHING = ["etlFlow", "targeting", "retraining"]


def combine_deployments_and_prefect_runs(
    df_prefect_runs: pd.DataFrame, deployment_specs: pd.DataFrame
):
    df_prefect_runs = df_prefect_runs.rename(
        columns={"Deployments": "Prefect_Deployments", "node_name": "prefect_node_name"}
    )
    prefect_runs_with_deployments = pd.DataFrame()
    for deployment in DEPLOYMENTS_MATCHING:
        deployment_specs_temp = deployment_specs[
            deployment_specs["Deployments"] == deployment
        ]
        df_prefect_runs_temp = df_prefect_runs[
            df_prefect_runs["Prefect_Deployments"] == deployment
        ]
        if deployment == "etlFlow":
            df_prefect_runs_temp["tenant"] = df_prefect_runs_temp["account"].apply(
                lambda x: sanitize_account_name(x)
            )
            merge_data = pd.merge(
                deployment_specs_temp, df_prefect_runs_temp, on="tenant", how="right"
            )
        elif deployment == "targeting" or deployment == "retraining":
            df_prefect_runs_temp["tenant"] = df_prefect_runs_temp["account"].apply(
                lambda x: sanitize_account_name(x)
            )
            merge_data = pd.merge(
                deployment_specs_temp,
                df_prefect_runs_temp,
                left_on=["tenant", "audience"],
                right_on=["tenant", "audience_id"],
                how="right",
            )
        else:
            raise ValueError(f"the deployment {deployment} is not defined.")
        prefect_runs_with_deployments = pd.concat(
            [prefect_runs_with_deployments, merge_data]
        )
    # for all others
    deployment_specs_temp = deployment_specs[
        deployment_specs["Deployments"].isin(DEPLOYMENTS_MATCHING) == False
    ]
    prefect_runs_with_deployments = pd.concat(
        [prefect_runs_with_deployments, deployment_specs_temp]
    )
    df_prefect_runs_temp = df_prefect_runs[
        df_prefect_runs["Prefect_Deployments"].isin(DEPLOYMENTS_MATCHING) == False
    ]
    prefect_runs_with_deployments = pd.concat(
        [prefect_runs_with_deployments, df_prefect_runs_temp]
    )
    return prefect_runs_with_deployments


def combine_data_with_node_name(
    df1: pd.DataFrame, df2: pd.DataFrame, merge_columns: list
):
    col_node_name = "node_name"
    if col_node_name not in df1.columns:
        raise ValueError(f"Dataframe 1 does not have the column {col_node_name}")
    if col_node_name not in df2.columns:
        raise ValueError(f"Dataframe 2 does not have the column {col_node_name}")
    df1 = df1[df1[col_node_name].isnull() == False]
    df2 = df2[df2[col_node_name].isnull() == False]
    merge = pd.merge(df1, df2, on=merge_columns, how="outer")
    return merge


def combine_data_without_node_name(df1: pd.DataFrame, df2: pd.DataFrame):
    col_node_name = "node_name"
    if col_node_name not in df1.columns:
        raise ValueError(f"Dataframe 1 does not have the column {col_node_name}")
    if col_node_name not in df2.columns:
        raise ValueError(f"Dataframe 2 does not have the column {col_node_name}")
    df1 = df1[df1[col_node_name].isnull()]
    df2 = df2[df2[col_node_name].isnull()]
    concat = pd.concat([df1, df2])
    return concat


def combine_prefect_runs_with_deployments_and_costs(
    prefect_runs_with_deployments: pd.DataFrame, costs: pd.DataFrame
):
    costs = costs.rename(columns={"start": col_date, "name": "node_name"})
    prefect_runs_with_deployments["prefect_node_name"] = np.where(
        prefect_runs_with_deployments["prefect_node_name"].isnull(),
        prefect_runs_with_deployments["node_name"],
        prefect_runs_with_deployments["prefect_node_name"],
    )
    prefect_runs_with_deployments = prefect_runs_with_deployments.rename(
        columns={"node_name": "deployment_node_name", "prefect_node_name": "node_name"}
    )
    merge_on_columns = ["node_name", col_date]

    df_with_nodes = combine_data_with_node_name(
        prefect_runs_with_deployments, costs, merge_on_columns
    )
    df_without_nodes = combine_data_without_node_name(
        prefect_runs_with_deployments, costs
    )
    concat = pd.concat([df_with_nodes, df_without_nodes])
    return concat


def concat_two_dataframes(exising_data: pd.DataFrame, azure_costs: pd.DataFrame):
    shape_existing = exising_data.shape
    shape_azure_costs = azure_costs.shape
    concat = pd.concat([exising_data, azure_costs])
    concat = concat.reset_index(drop=True)
    if concat.shape[0] != shape_azure_costs[0] + shape_existing[0]:
        raise ValueError(f"Shape does not match")
    return concat
