import awswrangler as wr
import pandas as pd
from general_functions.return_account_ids import return_account_ids
from general_functions.conncet_s3 import S3Connection


def load_bigquery_data(daterange: list, workspace: str):
    bigquery_data = pd.DataFrame()
    workspace_id = return_account_ids()
    workspace_id = [acc["id"] for acc in workspace_id if acc["name"] == workspace]
    if len(workspace_id) != 1:
        raise ValueError(f"Error in getting workspace: {workspace_id}")
    workspace_id = workspace_id[0]
    s3 = S3Connection()
    for date in daterange:
        date = pd.to_datetime(date).strftime("%Y%m%d")
        bigquery_path = f"bigQueryEvents/{date}/"
        files = s3.list_files(workspace_id, bigquery_path)
        if len(files) == 0:
            continue
        for file in files:
            temp = wr.s3.read_json(f"s3://{workspace_id}/{file}")
            bigquery_data = pd.concat([bigquery_data, temp])
    return bigquery_data


def main(daterange: list, workspace: str):
    bigquery_data = load_bigquery_data(daterange, workspace)
    return bigquery_data


if __name__ == "__main__":
    date = "20251201"
    date_end = "20260127"
    daterange = pd.date_range(date, date_end).strftime("%Y%m%d").tolist()
    workspace = "LILLYDOO"
    bigquery_data = main(daterange=daterange, workspace=workspace)
    bigquery_data.to_csv(
        "SprintStories/EN-3061-Lillydo-Abohistorie/bigquery_data.csv", index=False
    )
    print(bigquery_data)
