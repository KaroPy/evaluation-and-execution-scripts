import time
import pandas as pd
from datetime import datetime, timedelta
import awswrangler as wr
import concurrent.futures

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
res_path = f"{path_to_data}targeted_profiles_per_audience_{start_date}_{end_date}"

url = return_api_url()
accounts = return_account_ids()

s3 = S3Connection()


def process_file(args):
    file, account_id, account_name = args
    logger.info(f". file: {file}")
    if "meta" in str(file):
        return pd.DataFrame(
            columns=["account", "date", "audience_id", "unique_anonymous_ids"]
        )
    audience_id = file.split("/")[-1].split(".parquet")[0]
    for i in range(0, 3):
        try:
            temp = wr.s3.read_parquet(
                f"s3://{account_id}/{file}", columns=["anonymousId"]
            )
            break
        except Exception as e:
            if "botocore.exceptions.ReadTimeoutError" in str(e):
                time.sleep(30)
                print(f"try again - trial {i}")
            else:
                raise e
    unique_anonymous_ids = temp["anonymousId"].nunique()
    return pd.DataFrame(
        data={
            "account": [account_name],
            "date": [pd.to_datetime(file.split("/")[-2]).strftime("%Y-%m-%d")],
            "audience_id": [audience_id],
            "unique_anonymous_ids": [unique_anonymous_ids],
        }
    )


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
    targeting_history_prefixes = [
        file for file in targeting_history_prefixes if "meta" not in file
    ]
    # get only prefixes >= start_date
    logger.info(
        f". Filtered Prefixes {len(targeting_history_prefixes)}"
    )  # prefixes: {targeting_history_prefixes}"
    if len(targeting_history_prefixes) == 0:
        logger.info(". No prefixes found")
        return res_pd
    # iterate over prefixes
    for prefix in targeting_history_prefixes:
        date = prefix.split("/")[-2]
        logger.info(f". prefix: {prefix}, date: {date}")
        check_existence = res_pd[
            (res_pd["date"] == pd.to_datetime(date).strftime("%Y-%m-%d"))
            & (res_pd["account"] == account_name)
        ]
        if len(check_existence) > 0:
            logger.info(f". prefix: {prefix}, date: {date} already exists")
            continue
        files = s3.list_files_with_pagination(bucket_name=account_id, prefix=prefix)
        logger.info(f". Found {len(files)} files")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    process_file, [(file, account_id, account_name) for file in files]
                )
            )
        res_pd = pd.concat([res_pd] + results)
        res_pd.to_parquet(f"{saving_path}.parquet")
    res_pd.to_parquet(f"{saving_path}_{account_name}.parquet")
    logger.info(f". result: {res_pd.shape}")
    logger.info("###################")
    return res_pd


res = pd.read_parquet(f"{res_path}.parquet")
res = res.drop_duplicates().reset_index(drop=True)
for iaccount, account in enumerate(accounts):
    logger.info(f"Account {iaccount}/{len(accounts)}: {account['name']}")
    res = get_targeted_profiles_per_audience(account, start_date, res_path, res)
    res.to_parquet(f"{res_path}.parquet")
