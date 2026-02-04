import json
import requests
import pandas as pd
from datetime import datetime
from base64 import b64encode
from src.configs.data_specs import date_format_hashed, col_date
from src.utils.datetime_helper import transform_local_time_to_datetime
from src.utils.json_handling import write_data_to_json
from src.utils.constants import (
    return_prefect_self_hosted_auth_string,
    return_prefect_self_hosted_url,
    return_api_url_innkeepr,
)
from src.utils.innkeepr_api import call_api_with_service_token
from src.utils.accounts import sanitize_account_name

DEPLOYMENTS_TO_CONSIDER = ["etlFlow-k8s-"]
TAGS_TO_CONSIDER = ["analytics"]
# targeting, retraining and updateConversionTable, googleConversionUpdate: tag=analytics
def format_date_for_prefect_api(date: str):
    date = pd.to_datetime(date)
    return datetime.fromisoformat(str(date)).strftime("%Y-%m-%dT%H:%M:%SZ")


def call_prefect_api(endpoint: str, json_data: dict):
    api_url = return_prefect_self_hosted_url()
    secret = return_prefect_self_hosted_auth_string()
    secret = b64encode(secret.encode("utf-8"))
    auth_header = f"Basic {secret.decode('utf-8')}"

    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json",
    }

    response = requests.post(
        f"{api_url}{endpoint}",
        headers=headers,
        json=json_data,
        timeout=30,
    )

    if response.status_code != 200:
        response.raise_for_status()

    return response.json()


def get_account_name(x, accounts):
    for acc in accounts:
        sanitize_account = sanitize_account_name(acc["name"])
        if x == sanitize_account:
            return acc["name"]
    return None


class PrefectFlowViaApi:
    def __init__(self, logger):
        self.logger = logger

    def extract_task_runs(self, flow_run_ids: list):
        endpoint = f"/task_runs/filter"
        response = call_prefect_api(
            endpoint,
            {
                "flow_runs": {
                    "id": {
                        "any_": flow_run_ids,
                    },
                },
            },
        )
        task_runs = pd.json_normalize(response)
        return task_runs

    def extract_flow_runs_for_time_range(
        self, from_date: str, to_date: str, tags_to_consider=TAGS_TO_CONSIDER
    ):
        self.logger.info(f"extract_flow_runs_for_time_range: {from_date} - {to_date}")
        # Transform from_date and to_date to the required format: YYYY-MM-DDT00:00:00Z / YYYY-MM-DDT23:59:59Z

        from_date = pd.to_datetime(from_date).strftime(date_format_hashed)
        to_date = pd.to_datetime(to_date).strftime(date_format_hashed)
        from_date_formatted = f"{from_date}T00:00:00Z"
        to_date_formatted = f"{to_date}T23:59:59Z"
        date_format = "%Y-%m-%dT%H:%M:%SZ"
        self.logger.info(
            f"extract_flow_runs_for_time_range: {from_date_formatted} - {to_date_formatted}"
        )
        from_date_formatted = transform_local_time_to_datetime(
            from_date_formatted, date_format
        )
        to_date_formatted = transform_local_time_to_datetime(
            to_date_formatted, date_format
        )
        endpoint = "/flow_runs/filter"  # history"  # flow_runs/filter"
        filter_payload = {
            "flows": {"name": {"like_": "k8-targeting"}},
            "flow_runs": {
                "start_time": {
                    "after_": from_date_formatted,
                    "before_": to_date_formatted,
                }
            },
            "deployments": {
                # "name": {"like_": "etlFlow"},
                "tags": {"any_": tags_to_consider},
            },
        }
        self.logger.info(f"flow_runs/filter = {filter_payload}")
        response = call_prefect_api(endpoint, filter_payload)
        flow_runs = pd.json_normalize(response)
        return flow_runs

    def extract_deployments(self, deployment_ids: list):
        endpoint = "/deployments/filter"
        filter_payload = json.loads(
            json.dumps({"deployments": {"id": {"any_": deployment_ids}}})
        )
        response = call_prefect_api(endpoint, filter_payload)
        self.logger.info(f"response = {len(response)}")
        deployments = pd.json_normalize(response)
        deployments = deployments[["id", "name"]]
        deployments = deployments.rename(
            columns={"id": "deployment_id", "name": "deployment_name"}
        )
        deployments["deployment_string"] = deployments["deployment_name"].apply(
            lambda x: (
                "-".join(x.split("-")[0:2])
                if not "googleConversionUpdate" in x
                else x.split("-")[0]
            )
        )
        return deployments

    def clean_data(self, data: pd.DataFrame):
        use_columns = [
            "id",
            "name",
            "flow_id",
            "deployment_id",
            "start_time",
            "total_run_time",
            "parameters.tenant",
            "parameters.audience",
            "deployment_name",
            "deployment_string",
        ]
        data = data[use_columns]
        data[col_date] = pd.to_datetime(data["start_time"]).dt.date
        # rename data
        data = data.rename(
            columns={
                "parameters.audience": "audience_id",
                "deployment_name": "name",
                "start_time": "timestamp",
                "total_run_time": "duration",
            }
        )
        # TODO: account rename parameters.tenant to account
        url = return_api_url_innkeepr()
        accounts = call_api_with_service_token(
            endpoint_url=f"{url}/core/accounts/query", content={}, logger=self.logger
        )
        data["account"] = data["parameters.tenant"].apply(
            lambda x: get_account_name(x, accounts)
        )

        return data

    def extract_flows(
        self,
        from_date: str,
        to_date: str,
        save_data=False,
        tags_to_consider=TAGS_TO_CONSIDER,
    ):
        flow_runs = self.extract_flow_runs_for_time_range(
            from_date, to_date, tags_to_consider=tags_to_consider
        )
        self.logger.info(f"List flow runs: {len(flow_runs)}")
        list_deployments = flow_runs["deployment_id"].dropna().unique().tolist()
        self.logger.info(f"list_deployments = {len(list_deployments)}")
        deployments = self.extract_deployments(deployment_ids=list_deployments)
        flow_runs = pd.merge(flow_runs, deployments, on="deployment_id")
        data = self.clean_data(flow_runs)
        self.logger.info(f"data = {data.shape}")
        if save_data:
            data.to_csv(f"data_from_prefect_api_{from_date}_{to_date}.csv")
            # write_data_to_json(response, "test_data")
        return data
