import json
import boto3
import pandas as pd
from datetime import timedelta

from src.utils.json_handling import write_data_to_json
from src.configs.data_specs import date_format_hashed


class AWSExtractor:
    def __init__(self, logger, path_to_save="data/"):
        self.logger = logger
        self.path_to_save = path_to_save

    def query_costs_via_boto3(
        self, from_date, to_date, client=None, granularity="DAILY", save_data=False
    ):

        if client is None:
            client = boto3.client("ce")
        to_date = pd.to_datetime(to_date) + timedelta(days=1)
        to_date = to_date.strftime(date_format_hashed)
        date_range = list(
            pd.date_range(from_date, to_date, freq="5D").strftime(date_format_hashed)
        )
        if to_date not in date_range:
            date_range.append(to_date)
        results = {"ResultsByTime": []}
        for idate, from_date in enumerate(date_range):
            if idate + 1 == len(date_range):
                to_date = date_range[len(date_range) - 1]
            else:
                to_date = date_range[idate + 1]
            self.logger.info(f"Load aws costs from {from_date} to {to_date}")
            try:
                granularity = "DAILY"
                response = client.get_cost_and_usage(
                    TimePeriod={"Start": from_date, "End": to_date},
                    Granularity=granularity,
                    Metrics=["UnblendedCost", "UsageQuantity", "AmortizedCost"],
                    GroupBy=[
                        # {"Type": "DIMENSION", "Key": "AZ"},
                        {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
                        {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
                        # {"Type": "DIMENSION", "Key": "USAGE_TYPE_GROUP"},
                        # {"Type": "DIMENSION", "Key": "DATABASE_ENGINE"},
                        # {"Type": "DIMENSION", "Key": "INSTANCE_TYPE_FAMILY"},
                    ],
                )
                if "ResultsByTime" in response.keys():
                    results["ResultsByTime"].extend(response.get("ResultsByTime"))

                self.logger.info(f"Extracted {len(results)} cost records from AWS.")
                if save_data:
                    write_data_to_json(
                        results, f"{self.path_to_save}aws_costs_{from_date}_{to_date}"
                    )
            except Exception as e:
                self.logger.error(f"Failed to extract costs from AWS: {e}")

        results = json.loads(json.dumps(results))
        if save_data:
            write_data_to_json(results, f"{self.path_to_save}aws_costs_all")
        return results["ResultsByTime"]

    def preprocess_aws_json_data(self, json_data: json):
        data = pd.DataFrame(
            columns=[
                "date",
                "end",
                "serviceCategoryName",
                "serviceName",
                "charge",
                "billing_currency",
                "quantity",
                "quantity_unit",
                "cloud",
                "amortized_cost",
            ]
        )
        cloud = "aws"
        for entry in json_data:
            time_periods = entry.get("TimePeriod")
            if isinstance(time_periods, dict):
                date = time_periods.get("Start", None)
                end = time_periods.get("End", None)
            else:
                date = None
                end = None
            for group in entry.get("Groups"):
                service_group = group.get("Keys")
                serviceCategoryName = service_group[0]
                if len(service_group) == 2:
                    serviceName = service_group[1]
                else:
                    raise ValueError(
                        f"More than two services are not defined to handle {service_group}"
                    )
                currency = group.get("Metrics").get("UnblendedCost").get("Unit")
                costs = group.get("Metrics").get("UnblendedCost").get("Amount")
                quantity = group.get("Metrics").get("UsageQuantity").get("Amount")
                quantity_unit = group.get("Metrics").get("UsageQuantity").get("Unit")
                amortized_cost = group.get("Metrics").get("AmortizedCost").get("Amount")
                data.loc[len(data)] = [
                    date,
                    end,
                    serviceCategoryName,
                    serviceName,
                    costs,
                    currency,
                    quantity,
                    quantity_unit,
                    cloud,
                    amortized_cost,
                ]

        use_columns = [
            "date",
            "end",
            "serviceName",
            "serviceCategoryName",
            "charge",
            "quantity",
            "billing_currency",
            "cloud",
            "amortized_cost",
        ]
        data = data[use_columns]
        return data

    def extract_costs(
        self, from_date, to_date, client=None, granularity="DAILY", save_data=True
    ):
        """
        Extracts cost data from AWS Cost Explorer API.

        Args:
            client: boto3 Cost Explorer client.
            from_date (str): Start date in 'YYYY-MM-DD' format.
            to_date (str): End date in 'YYYY-MM-DD' format.
            granularity (str): Granularity of the data ('DAILY', 'MONTHLY').

        Returns:
            list: List of cost results.
        """
        json_data = self.query_costs_via_boto3(from_date, to_date, save_data=save_data)
        if len(json_data) == 0:
            raise ValueError(f"No AWS costs were found: {json_data}")
        aws_costs_pd = self.preprocess_aws_json_data(json_data)
        if save_data:
            aws_costs_pd.to_csv(
                f"{self.path_to_save}aws_costs_pd_{from_date}_to_{to_date}.csv"
            )
        return aws_costs_pd
