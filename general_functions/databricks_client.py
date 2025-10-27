import os
import time
import json
import logging
import requests
import delta_sharing
from dotenv import load_dotenv
from general_functions.constants import (
    return_databricks_api_token,
    return_databricks_url,
)

load_dotenv()

MAX_TRIES = 3
SLEEP_TIME = 30


def return_databricks_client():
    """
    Function to return a databricks client
    Parameters
    ----------
    None
    Returns
    -------
    profile_path: delta_sharing.SharingClient
    """
    # TODO: unittest
    profile_path_temp = "configs/databricks_template.share"
    bearer_token = os.environ["BEARER_TOKEN"]
    profile_path = "temp.json"

    # Load share template
    # Load profile_path_temp as json
    with open(profile_path_temp, "r") as file:
        profile_temp = json.load(file)

    # Change the value of bearerToken
    profile_temp["bearerToken"] = bearer_token

    # Save it to profile_path
    with open(profile_path, "w") as file:
        json.dump(profile_temp, file)

    return profile_path


def delete_databricks_profile_path(profile_path):
    os.remove(profile_path)


def load_delta_as_pandas(profile_path, table_name, limit_val=None):
    """
    Function to load a table from delta sharing
    Parameters
    ----------
    profile_path: str
    table_name: str
    Returns
    -------
    df: pandas.DataFrame
    """
    trial = 0
    while trial < MAX_TRIES:
        try:
            if limit_val is None:
                df = delta_sharing.load_as_pandas(f"{profile_path}#{table_name}")
            else:
                logging.warning(f"Loading {limit_val} rows from Databricks table")
                df = delta_sharing.load_as_pandas(
                    f"{profile_path}#{table_name}", limit=limit_val
                )
            break
        except Exception as e:
            logging.warning(f"Error in accessing Databricks table: {e}")
            time.sleep(SLEEP_TIME)
            trial += 1
        if trial == MAX_TRIES:
            delete_databricks_profile_path(profile_path)
            raise Exception("Error in accessing Databricks table")
    return df


def extract_cluster_info(cluster_id: str):
    databricks_url = return_databricks_url()
    token = return_databricks_api_token()
    endpoint = f"{databricks_url}api/2.1/clusters/get"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cluster_id": cluster_id}

    response = requests.get(endpoint, headers=headers, params=params)
    if response.status_code != 200:
        if f"Cluster {cluster_id} does not exist" in response.text:
            return {"driver_node_type_id": None}
        raise Exception(f"Failed to list jobs: {response.text}")

    return response.json()
