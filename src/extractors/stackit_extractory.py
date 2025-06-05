import requests
import pandas as pd
import json
from datetime import timedelta
from src.utils.constants import (
    get_stackit_token,
    get_stackit_kubernetes_project_id,
    get_stackit_customer_account_id,
)
from src.configs.data_specs import date_format_hashed
from kubernetes import client, config as k8_config

STACKIT_K8S_URL = "https://api.tolkien.f8c5685e23.s.ske.eu01.onstackit.cloud"


class StackItExtractor:
    def __init__(self, logger, path_to_save="data/"):
        self.token = get_stackit_token()
        self.project_id = get_stackit_kubernetes_project_id()
        self.customer_account_id = get_stackit_customer_account_id()
        self.logger = logger
        self.path_to_save = path_to_save

    def transform_costs_to_pd(self, json_data: json):
        res = pd.DataFrame(
            columns=[
                "serviceName",
                "serviceCategoryName",
                "start",
                "end",
                "charge",
                "quantity",
            ]
        )
        for entry in json_data["services"]:
            serviceName = entry["serviceName"]
            serviceCategoryName = entry["serviceCategoryName"]
            for data in entry["reportData"]:
                start = data["timePeriod"]["start"]
                end = data["timePeriod"]["end"]
                charge = data["charge"]
                quantity = data["quantity"]
                res.loc[len(res)] = [
                    serviceName,
                    serviceCategoryName,
                    start,
                    end,
                    charge,
                    quantity,
                ]
        return res

    def extract_costs_for_daterange(
        self, from_date: str, to_date: str, save_as_json=False
    ):
        costs_url = f"https://cost.api.stackit.cloud/v3/costs/{self.customer_account_id}/projects/{self.project_id}"
        date_range = list(
            pd.date_range(from_date, to_date, freq="30D").strftime(date_format_hashed)
        )
        if to_date not in date_range:
            date_range.append(to_date)
        print(date_range)
        save_as_json = True
        for idate, from_date in enumerate(date_range):
            if idate + 1 == len(date_range):
                to_date = date_range[len(date_range) - 1]
            else:
                to_date = date_range[idate + 1]
                to_date = pd.to_datetime(to_date) - timedelta(days=1)
                to_date = to_date.strftime(date_format_hashed)
            self.logger.info(f"Load stackit costs for {from_date} - {to_date}")
            response = requests.get(
                f"{costs_url}?from={from_date}&to={to_date}&granularity=daily&depth=service",
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=60,
            )
            if response.status_code != 200:
                raise Exception(
                    f"Failed to fetch costs: {response.status_code} - {response.text}"
                )
            response = response.json()
            if idate == 0:
                all_response = response
            else:
                all_response["services"].extend(response["services"])
            if save_as_json:
                with open(f"{self.path_to_save}stackit_costs_{idate}.json", "w") as f:
                    json.dump(response, f, indent=2)
        if save_as_json:
            with open(f"{self.path_to_save}stackit_costs_all.json", "w") as f:
                json.dump(all_response, f, indent=2)
        cost = pd.DataFrame()
        cost = self.transform_costs_to_pd(all_response)
        # for entry in response["services"]:
        #     service_pd = pd.json_normalize(entry)
        #     cost = pd.concat([cost, service_pd], ignore_index=True)
        return cost

    def get_kubernetes_clusters(self, cluster_name="tolkien"):
        url = f"{STACKIT_K8S_URL}/kubernetes/clusters"
        url = (
            f"https://ske.api.eu01.stackit.cloud/v1/projects/{self.project_id}/clusters"
        )
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=60,
        )
        if response.status_code != 200:
            raise NotImplementedError(
                f"Failed to fetch Kubernetes clusters: {response.status_code} - {response.text}"
            )
        for entry in response.json().get("items", []):
            if entry.get("name") == cluster_name:
                return entry
        raise NotImplementedError(f"Cluster with name {cluster_name} not found")

    def extract_node_types_from_cluster(self, cluster: json):
        self.logger.info("extract_node_types_from_cluster")
        node_pools = cluster.get("nodepools", None)
        if node_pools is None:
            raise NotImplementedError("No node pools found in the cluster")
        node_pools_df = pd.DataFrame()
        for entry in node_pools:
            temp = pd.json_normalize(entry)
            node_pools_df = pd.concat([node_pools_df, temp], ignore_index=True)
        node_pools_df = node_pools_df[
            node_pools_df["name"] != "agentpool"
        ]  # TODO: for now it works, but it is not dynamic
        return node_pools_df

    def merge_costs_with_nodes(self, costs: pd.DataFrame, node_pools: pd.DataFrame):
        self.logger.info("merge_costs_with_nodes")
        columns_nodes = "machine.type"
        columns_node_name = "name"
        columns_volumne_type = "volume.type"
        columns_service = "serviceName"
        node_types = node_pools[columns_nodes].dropna().unique()
        self.logger.info(f"Node types: {node_types}")
        # For each node type, if its value is found in serviceName, set machine.type to that value
        costs[columns_nodes] = None
        for node_type in node_types:
            mask = costs[columns_service].str.contains(f"-{str(node_type)}-", na=False)
            costs.loc[mask, columns_nodes] = node_type

        costs = pd.merge(
            costs,
            node_pools[[columns_nodes, columns_node_name, columns_volumne_type]],
            left_on=columns_nodes,
            right_on=columns_nodes,
            how="left",
        )
        return costs

    def preprocess_data(self, data: pd.DataFrame):
        data["cost_per_unity"] = data["charge"] / data["quantity"]
        data["days"] = (
            pd.to_datetime(data["start"]) - pd.to_datetime(data["end"])
        ).dt.days
        check_nr_days = (data["days"] != 0).sum()
        if check_nr_days > 0:
            raise ValueError("Costs over several days are in the data.")
        data["cloud"] = "stackit"
        data["charge"] = data["charge"] / 100  # transform from cents to euro
        return data

    def extract_costs(self, from_date: str, to_date: str):
        stackit_costs = self.extract_costs_for_daterange(from_date, to_date)
        clusters = self.get_kubernetes_clusters()
        node_pools = self.extract_node_types_from_cluster(clusters)
        stackit_costs = self.merge_costs_with_nodes(stackit_costs, node_pools)
        stackit_costs = self.preprocess_data(stackit_costs)
        return stackit_costs
