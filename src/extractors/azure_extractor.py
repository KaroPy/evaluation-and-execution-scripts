import pandas as pd
from azure.mgmt.consumption import ConsumptionManagementClient
from azure.identity import DefaultAzureCredential

from src.utils.constants import (
    return_azure_subscription_id,
)
from src.utils.json_handling import write_data_to_json
from src.configs.data_specs import date_format_hashed
import json


class AzureExtractor:
    def __init__(self, logger, path_to_save="data/"):
        self.logger = logger
        self.path_to_save = path_to_save

    def perprocess_azure_json_data(self, data: json):
        data = pd.json_normalize(data)
        data = data.rename(
            columns={
                "product": "serviceName",
                "consumed_service": "serviceCategoryName",
                "cost": "charge",
            }
        )
        data["cloud"] = "azure"
        use_columns = [
            "date",
            "serviceName",
            "serviceCategoryName",
            "charge",
            "quantity",
            "effective_price",
            "billing_currency",
            "resource_id",
            "resource_name",
            "resource_group",
            "cloud",
        ]
        data = data[use_columns]
        return data

    def query_costs_via_client(self, from_date: str, to_date: str, save_data=False):
        start_date_str = pd.to_datetime(from_date).strftime(date_format_hashed)
        end_date_str = pd.to_datetime(to_date).strftime(date_format_hashed)
        subscription_id = return_azure_subscription_id()
        credential = DefaultAzureCredential()
        consumption_client = ConsumptionManagementClient(
            credential=credential, subscription_id=subscription_id
        )
        scope = f"/subscriptions/{subscription_id}"
        filter_expression = f"properties/usageStart ge '{start_date_str}' and properties/usageStart le '{end_date_str}'"

        usage_details = consumption_client.usage_details.list(
            scope=scope, filter=filter_expression
        )
        usage_list = [usage.as_dict() for usage in usage_details]
        write_data_to_json(
            usage_list, f"{self.path_to_save}azure_usage_details_{from_date}_{to_date}"
        )
        return usage_list

    def get_costs(self, from_date=None, to_date=None, save_data=False):
        json_costs = self.query_costs_via_client(
            from_date=from_date, to_date=to_date, save_data=False
        )
        azure_costs_pd = self.perprocess_azure_json_data(json_costs)
        if save_data:
            azure_costs_pd.to_csv(
                f"{self.path_to_save}azure_costs_pd_{from_date}_{to_date}.csv"
            )
        return azure_costs_pd
