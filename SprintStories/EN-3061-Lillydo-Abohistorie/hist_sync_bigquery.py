import requests
import pandas as pd
from general_functions.return_account_ids import (
    return_account_ids,
    return_service_token,
)
from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.constants import return_api_url


def hist_sync_bigquery(daterange: list, workspace: str):
    api_url = "https://api.innkeepr.ai/api"  # return_api_url()
    token = return_service_token()
    workspace_id = return_account_ids()
    workspace_id = [acc["id"] for acc in workspace_id if acc["name"] == workspace]
    if len(workspace_id) != 1:
        raise ValueError(f"Error in getting workspace: {workspace_id}")
    workspace_id = workspace_id[0]
    for date in daterange:
        print(f"{api_url}/sources/googleBigQuery/events/extract")
        response = requests.post(
            f"{api_url}/sources/googleBigQuery/events/extract",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "content": {"date": str(date)},
                "context": {"workspaceId": workspace_id},
            },
        )
        print(f"date {date} response: {response.status_code}")
        if response.status_code > 299:
            raise Exception(
                response.status_code, f"Unexpected server response: {response.text}"
            )


def main(daterange: list, workspace: str):
    hist_sync_bigquery(daterange, workspace)


if __name__ == "__main__":
    date = "20251201"
    date_end = "20260115"
    daterange = pd.date_range(date, date_end).strftime("%Y%m%d").tolist()
    workspace = "LILLYDOO"
    main(daterange=daterange, workspace=workspace)
