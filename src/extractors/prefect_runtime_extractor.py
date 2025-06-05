from enum import Enum
import pandas as pd
import numpy as np

from src.utils.databricks_connection import DatabricksClient
from src.utils.prefect_api import PrefectFlowViaApi
from src.utils.directory_handling import create_directory_if_not_exists
from src.utils.constants import return_databricks_shared_table
import src.configs.data_specs as data_specs

SCHEMA_DATABRICKS = "monitoring"
NAME_FILE_TABLE_PREFECT_LOGS = "prefect_data"


class DataSourceType(Enum):
    LOCAL_FILE = "local"
    DATABRICKS_PREFECT_LOGS = "databricks_prefect_logs"
    PREFECT_HOST = "prefect_host"
    PREFECT_HOST_OLD = "prefect_host_old"


class DataSourceName(Enum):
    DATABRICKS_PREFECT_LOGS = "prefect_logs"


class DataPropertiesToUse(Enum):
    DATABRICKS_PREFECT_LOGS = [
        "name",
        "timestamp",
        "audience_id",
        "account",
        # "account_id",
        "duration",
        "type",
        # "goal",
        # "goal_id",
        # "size",
        # "targeting_outlook",
        # "conversion_probability_cgroup",
        # "conversion_probability_tgroup",
        # "f1_causal",
        # "f1_conversion",
        # "targeted_users",
        # "total_users_in_percentage",
        "status",
        "audience_type",
        # "source",
        "node_name",
    ]


class CleanDataPrefectRuntime:
    def __init__(self, logger):
        self.logger = logger

    def clean_databricks_prefect_logs(self, df):
        col_date = data_specs.col_timestamp
        col_new_date = data_specs.col_date
        df[col_new_date] = pd.to_datetime(df[col_date]).dt.strftime(
            data_specs.date_format_hashed
        )
        df = df.rename(columns={"name": "Deployments"})
        return df


class PrefectRuntimeExtractor:
    def __init__(self, logger, path_save="data/"):
        self.logger = logger
        self.path_save = path_save
        create_directory_if_not_exists(self.path_save)

    def extract_data(self, data_source, from_date: str, to_date: str, save_data=False):
        clean_data = CleanDataPrefectRuntime(self.logger)
        if data_source == DataSourceType.DATABRICKS_PREFECT_LOGS.value:
            db_client = DatabricksClient(self.logger)
            catalog = return_databricks_shared_table()
            table = f"{catalog}.{SCHEMA_DATABRICKS}.{DataSourceName.DATABRICKS_PREFECT_LOGS.value}"
            df = db_client.load_delta_as_pandas(table_name=table)
            columns = DataPropertiesToUse.DATABRICKS_PREFECT_LOGS.value
            df = df[columns]
            self.logger.info(f"Loaded data: {df.shape}")
            df = clean_data.clean_databricks_prefect_logs(df)
            return df
        elif data_source == DataSourceType.LOCAL_FILE.value:
            df = pd.read_csv(f"{self.path_save}{NAME_FILE_TABLE_PREFECT_LOGS}.csv")
            return df
        elif data_source == DataSourceType.PREFECT_HOST.value:
            prefect = PrefectFlowViaApi(self.logger)
            df = prefect.extract_flows(from_date, to_date, save_data=save_data)
            return df
        else:
            raise ValueError(f"The data source {data_source} is not defined.")

    def clean_data(self, df: pd.DataFrame, from_date: str, to_date: str):
        col_date = data_specs.col_date
        df[col_date] = pd.to_datetime(df[col_date]).dt.date
        df[col_date] = df[col_date].astype("string")
        from_date = pd.to_datetime(from_date).strftime(data_specs.date_format_hashed)
        to_date = pd.to_datetime(to_date).strftime(data_specs.date_format_hashed)
        df = df[(df[col_date] >= from_date) & (df[col_date] <= to_date)]
        self.logger.info(f"After cleaning the data: {df.shape}")
        min_date = df[col_date].min()
        max_date = df[col_date].max()
        self.logger.info(f"Dates ranges from {min_date} to {max_date}")
        df["Deployments"] = np.where(
            df["Deployments"] == "k8-retraining", "retrainng", df["Deployments"]
        )
        df["Deployments"] = np.where(
            df["Deployments"] == "k8-targeting", "targeting", df["Deployments"]
        )
        return df

    def extract_runtimes(
        self,
        from_date: str,
        to_date: str,
        data_source="databricks_prefect_logs",
        save_data=False,
    ):
        # TODO: extract data from databricks table
        df = self.extract_data(
            data_source=data_source,
            from_date=from_date,
            to_date=to_date,
            save_data=save_data,
        )
        df = self.clean_data(df=df, from_date=from_date, to_date=to_date)
        if save_data:
            df.to_csv(
                f"{self.path_save}{NAME_FILE_TABLE_PREFECT_LOGS}_{from_date}_{to_date}_{data_source}.csv"
            )

        return df
