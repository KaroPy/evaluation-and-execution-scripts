import pandas as pd
from databricks.sdk import AccountClient

from src.utils.constants import (
    return_databricks_account_id,
)
from src.configs.data_specs import col_date


class DatabricksUsageExtractor:
    def __init__(
        self,
        logger,
        path_to_save="data/",
    ):
        """ """
        self.logger = logger
        self.path_to_save = path_to_save
        # self.account_id = return_databricks_account_id()
        # self.token = return_databricks_bearer_token()

        # databricks_client = AccountClient(
        #     host="https://accounts.cloud.databricks.com",
        #     account_id=return_databricks_account_id(),
        #     client_id="29a6338b-9510-4f72-949c-0f4e42e6d9b8",
        #     client_secret="",
        # )
        # for g in databricks_client.groups.list():
        #     print(g.display_name)
        # databricks_client.billable_usage.download("2025-05", "2025-06")
        # #TODO: does not return usage

    def load_local_files(self, from_date: str, to_date: str):
        cluster_id_file = (
            "data/downloaded_databricks_costs/20250605_databricks_costs_cluster_id.csv"
        )
        cluster_data = pd.read_csv(cluster_id_file)
        cluster_data = cluster_data.rename(columns={"rank_key": "cluster_id"})
        cluster_data["resource_group"] = "cluster"
        self.logger.info(f"cluster_data: {cluster_data.shape}")

        resource_file = (
            "data/downloaded_databricks_costs/20250605_databricks_costs_grouped.csv"
        )
        resource_data = pd.read_csv(resource_file)
        resource_data = resource_data.rename(columns={"group_key": "resource_name"})
        resource_data["resource_group"] = "resource"
        self.logger.info(f"resource_data: {resource_data.shape}")

        node_type_file = (
            "data/downloaded_databricks_costs/20250605_databricks_costs_node_types.csv"
        )
        node_types = pd.read_csv(node_type_file)
        node_types = node_types.rename(columns={"rank_key": "machine.type"})
        node_types["resource_group"] = "node_types"
        self.logger.info(f"node_types: {node_types.shape}")

        result = pd.concat([cluster_data, resource_data])
        self.logger.info(f"Merge cluster and resource: {result.shape}")
        result = pd.concat([result, node_types])
        self.logger.info(f"Merge result and nodes: {result.shape}")

        result = result.rename(
            columns={"time_key": col_date, "sum(usage_usd)": "charge"}
        )
        if len(result) != sum([len(cluster_data), len(resource_data), len(node_types)]):
            raise ValueError(
                f"Lenght of data {len(result)} is not as expected: {max(len(cluster_data), len(resource_data), len(node_types))}"
            )
        result = result[(result[col_date] >= from_date)]
        result = result[(result[col_date] <= to_date)]
        result["cloud"] = "databricks"
        result["billing_currency"] = "USD"
        self.logger.info(
            f"result date range: {result[col_date].min()} -  {result[col_date].max()}"
        )
        return result

    def load_cost(
        self, from_date: str, to_date: str, local_files=True, save_data=False
    ):
        if local_files is False:
            raise ValueError(f"Other way than local_files is not defined.")

        databricks_costs = self.load_local_files(from_date, to_date)
        if save_data:
            databricks_costs.to_csv(
                f"{self.path_to_save}databricks_data_{from_date}_{to_date}.csv"
            )
        return databricks_costs
