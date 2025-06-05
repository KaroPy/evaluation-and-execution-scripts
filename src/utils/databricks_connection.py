import os
import json
import logging
import time
import delta_sharing

from src.utils.constants import return_databricks_token

DATABRICKS_TEMPLATE_FILE = "src/configs/databricks_template.share"
DATABRICKS_PROFILE_FILE = "temp_databricks.json"
MAX_TRIES = 3
SLEEP_TIME = 30


class DatabricksClient:
    def __init__(
        self,
        logger,
        template_path=DATABRICKS_TEMPLATE_FILE,
        profile_path=DATABRICKS_PROFILE_FILE,
    ):
        self.logger = logger
        self.template_path = template_path
        self.profile_path = profile_path

    def create_profile(self):
        bearer_token = return_databricks_token()
        with open(self.template_path, "r") as file:
            profile_temp = json.load(file)
        profile_temp["bearerToken"] = bearer_token
        with open(self.profile_path, "w") as file:
            json.dump(profile_temp, file)
        return self.profile_path

    def delete_profile(self):
        if self.profile_path and os.path.exists(self.profile_path):
            os.remove(self.profile_path)
            self.profile_path = None

    def load_delta_as_pandas(
        self, table_name, limit_val=None, max_tries=MAX_TRIES, sleep_time=SLEEP_TIME
    ):
        profile_path = self.create_profile()
        table_name = f"{profile_path}#{table_name}"
        params = {"url": table_name}
        if limit_val is not None:
            params["limit"] = limit_val
        trial = 0
        while trial < max_tries:
            try:
                self.logger.info(f"Load data with {params}")
                df = delta_sharing.load_as_pandas(**params)
                self.delete_profile()
                return df
            except Exception as e:
                self.logger.warning(
                    f"Error in accessing Databricks table {table_name}: {e}"
                )
                time.sleep(sleep_time)
                trial += 1
        self.delete_profile()
        raise Exception("Error in accessing Databricks table")
