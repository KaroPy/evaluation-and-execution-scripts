import logging
import pandas as pd
from general_functions.call_api_with_account_id import call_api_with_accountId

DES_SOURCES = ["facebook", "googleAnalytics", "tiktok"]


def return_all_audiences(api_url: str, account_id: str, logger: logging):
    audiences = call_api_with_accountId(
        f"{api_url}/audiences/query", account_id, {}, logger
    )
    audiences = pd.json_normalize(audiences)
    if audiences.empty:
        return audiences
    sources = call_api_with_accountId(
        f"{api_url}/sources/query", account_id, {}, logger
    )
    sources = pd.json_normalize(sources)
    sources = sources[sources["name"].isin(DES_SOURCES)]
    audiences = audiences[audiences["source"].isin(sources["id"])]
    return audiences
