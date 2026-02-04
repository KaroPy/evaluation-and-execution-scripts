import os
import json
from dotenv import load_dotenv

load_dotenv()
REPOSITORY_TARGETING = "prefect-2-targeting"
REPOSITRY_ETLFLOW = "prefect-flows-etl"


def return_api_url_innkeepr():
    return "https://targeting.innkeepr.ai/api"


def return_azure_credential():
    return json.loads(os.environ["AZURE_CREDENTIAL"])


def return_azure_client_id():
    return os.environ["AZURE_CLIENT_ID"]


def return_azure_client_secrets():
    return os.environ["AZURE_CLIENT_SECRET"]


def return_azure_subscription_id():
    return os.environ["AZURE_SUBSCRIPTION_ID"]


def return_azure_tenant_id():
    return os.environ["AZURE_TENANT_ID"]


def return_databricks_shared_table():
    return "delta_share_events"


def return_databricks_account_id():
    return "df53fe19-0612-449e-9860-325c44fe88fb"


def return_databricks_client_secret():
    return os.environ["DATABRICKS_CLIENT_SECRET"]


def return_databricks_token():
    return os.environ["DATABRICKS_DATA_TOKEN"]


def get_github_token():
    return os.environ["GITHUB_TOKEN"]


def return_prefect_account_id():
    return os.environ["PREFECT_ACCOUNT_ID"]


def return_prefect_workspace_id():
    return os.environ["PREFECT_WORKSPACE_ID"]


def return_prefect_self_hosted_auth_string():
    return os.environ["PREFECT_API_KEY"]


def return_prefect_self_hosted_url():
    return "https://prefect.innkeepr.ai/api"


def return_repo_name_targeting():
    return REPOSITORY_TARGETING


def return_repo_name_etlflow():
    return REPOSITRY_ETLFLOW


def return_service_token():
    return os.environ["API_SERVICE_TOKEN"]


def get_stackit_kubernetes_project_id():
    return "89645e0c-4325-4056-ba15-7723a3e0264a"


def get_stackit_customer_account_id():
    return "f0340223-33be-4c8a-a85c-10c96b401711"


def get_stackit_token():
    return os.environ["STACKIT_TOKEN"]
