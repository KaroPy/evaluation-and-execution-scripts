import json
import requests

from general_functions.constants import return_service_token, return_api_url


def return_account_ids(tracking_started=True):
    url = return_api_url()
    url = f"{url}/core/accounts/query"
    print(url)

    payload = json.dumps(
        {"content": {}, "context": {"serviceToken": return_service_token()}}
    )
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {return_service_token()}",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if tracking_started:
        accounts = [
            entry
            for entry in response.json()["data"]
            if entry["trackingOptions"]["eventTrackingStarted"] == True
        ]
    else:
        accounts = response.json()["data"]
    accounts = [{"id": entry["id"], "name": entry["name"]} for entry in accounts]
    return accounts


def return_accounts():
    url = return_api_url()
    url = f"{url}/core/accounts/query"
    print(url)

    payload = json.dumps(
        {"content": {}, "context": {"serviceToken": return_service_token()}}
    )
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {return_service_token()}",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    accounts = [
        entry
        for entry in response.json()["data"]
        if entry["trackingOptions"]["eventTrackingStarted"] == True
    ]
    return accounts
