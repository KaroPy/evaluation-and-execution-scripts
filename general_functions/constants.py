import os
from dotenv import load_dotenv

load_dotenv()


def return_service_token():
    return os.environ["SERVICE_TOKEN"]


def return_api_url():
    return os.environ["URL"]


def return_api_url_prod():
    return os.environ["URL_PROD"]


def return_databricks_url():
    return "https://dbc-4ed91336-7d96.cloud.databricks.com/"


def return_databricks_bearer_token():
    return os.environ["DATABRICKS_BEARER_TOKEN"]


def return_databricks_api_token():
    return os.environ["DATABRICKS_API_TOKEN"]


def return_databricks_url():
    return "https://dbc-4ed91336-7d96.cloud.databricks.com/"
