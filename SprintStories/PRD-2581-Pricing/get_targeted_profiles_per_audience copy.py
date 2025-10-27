import pandas as pd
from datetime import datetime, timedelta
import awswrangler as wr

from general_functions.define_logging import define_logging
from general_functions.conncet_s3 import S3Connection
from general_functions.return_account_ids import return_account_ids
from general_functions.constants import return_api_url

path_to_data = "SprintStories/PRD-2581-Pricing/"
logger = define_logging(
    f"{path_to_data}logs/get_targeted_profiles_per_audience-{datetime.now()}"
)
end_date = datetime.today().date() + timedelta(days=10)
start_date = end_date - timedelta(days=31 * 6)
logger.info(f"start_date: {start_date}, end_date: {end_date}")

res = pd.DataFrame(columns=["account", "date", "audience_id", "unique_anonymous_ids"])
res_path = (
    f"{path_to_data}targeted_profiles_per_audience_{start_date}_{end_date}.parquet"
)

url = return_api_url()
accounts = return_account_ids()

s3 = S3Connection()


def get_targeted_profiles_per_audience(account_dict, min_date, saving_path, res_pd):
    account_name = account_dict["name"]
    account_id = account_dict["id"]
    logger.info(f"Account: {account_name}")
    s3_targeting_history_prefix = f"targeting.history/"
    # list prefixes in targeting history
    targeting_history_prefixes = s3.list_files_with_pagination(
        bucket_name=account_id, prefix=s3_targeting_history_prefix, delimiter="/"
    )
    logger.info(
        f". Found {len(targeting_history_prefixes)}"
    )  # prefixes: {targeting_history_prefixes}"
    check_min_date = pd.to_datetime(min_date).strftime("%Y%m%d")
    logger.info(f". Check min date: {check_min_date}")
    targeting_history_prefixes = [
        prefix
        for prefix in targeting_history_prefixes
        if prefix.split("/")[-2] >= str(check_min_date)
    ]
    # get only prefixes >= start_date
    logger.info(
        f". Filtered Prefixes {len(targeting_history_prefixes)}"
    )  # prefixes: {targeting_history_prefixes}"
    if len(targeting_history_prefixes) == 0:
        logger.info(". No prefixes found")
        return pd.DataFrame(
            columns=["account", "date", "audience_id", "unique_anonymous_ids"]
        )
    # iterate over prefixes
    for prefix in targeting_history_prefixes:
        date = prefix.split("/")[-2]
        logger.info(f". prefix: {prefix}, date: {date}")
        files = s3.list_files_with_pagination(bucket_name=account_id, prefix=prefix)
        logger.info(f". Found {len(files)}")  # files: {files}")
        for file in files:
            logger.info(f". file: {file}")
            audience_id = file.split("/")[-1].split(".parquet")[0]
            temp = wr.s3.read_parquet(
                f"s3://{account_id}/{file}", columns=["anonymousId"]
            )
            unique_anonymous_ids = temp["anonymousId"].nunique()
            # res = pd.DateFrame(columns=["account", "date", "audience_id", "unique_anonymous_ids"])
            temp = pd.DataFrame(
                data={
                    "account": [account_name],
                    "date": [pd.to_datetime(date).strftime("%Y-%m-%d")],
                    "audience_id": [audience_id],
                    "unique_anonymous_ids": [unique_anonymous_ids],
                }
            )
            res_pd = pd.concat([res_pd, temp])
        res_pd.to_parquet(f"{saving_path}")
        break
    res_pd.to_parquet(f"{saving_path}")
    logger.info(f". result: {res_pd.shape}")
    logger.info("###################")
    return res_pd


for iaccount, account in enumerate(accounts[0:2]):
    logger.info(f"Account {iaccount}/{len(accounts)}: {account['name']}")
    res = get_targeted_profiles_per_audience(account, start_date, res_path, res)
