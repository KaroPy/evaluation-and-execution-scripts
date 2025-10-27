from datetime import datetime
from general_functions.conncet_s3 import S3Connection
from general_functions.define_logging import define_logging

logger = define_logging(
    f"sync-scripts/new_bucket_infrasturcture/check_for_wront_conversion_probability_files-{datetime.now()}"
)
remove_buckets = [
    "cf-templates-ixgvmtgxtpk1-eu-central-1",
    "conversion-probabilities",
    "data-docs.innkeepr-development",
    "databricks-s3-ingest-95f55-lambdazipsbucket-qu1i122fghbn",
    "databricks-workspace-stack-00540-bucket",
    "databricks-workspace-stack-00540-lambdazipsbucket-nzajynxumzs0",
    "elasticbeanstalk-eu-central-1-663925627205",
    "innkeepr-analytics-trainings",
    "innkeepr-bucket-logs",
    "innkeepr-cloudformationstack",
    "innkeepr-customer-repro-update",
    "innkeepr-databricks-delta-sharing",
    "innkeepr-development",
    "innkeepr-loadbalancer-production",
    "innkeepr-loadbalancer-targeting-production",
    "innkeepr-pixel",
    "innkeepr-prefect-flows",
    "innkeepr-prefect-flows-targeting",
    "innkeepr-production",
    "innkeepr-usage-report",
]
s3 = S3Connection()

all_buckets = s3.list_buckets()
logger.info(f"Found {len(all_buckets)} buckets")
all_buckets = [
    bucket
    for bucket in all_buckets
    if "innkeepr-targeting" not in bucket and "test" not in bucket
]
for rm_bucket in remove_buckets:
    if rm_bucket in all_buckets:
        all_buckets.remove(rm_bucket)
logger.info(f"Found {all_buckets} buckets")
for bucket in all_buckets:
    logger.info(f"Bucket: {bucket}")
    prefixes = s3.list_files_with_pagination(bucket, prefix="", delimiter="/")
    logger.info(f"Found {len(prefixes)} prefixes: {prefixes}")
    if f"{bucket}/" in prefixes:
        logger.info(f"Bucket {bucket} is a prefix")
        sub_prefixes = s3.list_files_with_pagination(bucket, prefix=f"{bucket}/")
        sub_prefixes = [
            prefix for prefix in sub_prefixes if "targeting.history" in prefix
        ]
        logger.info(f"Found {len(sub_prefixes)} sub prefixes: {sub_prefixes}")
        for sub_pref in sub_prefixes:
            logger.info(f". Prefix: {sub_pref}")
            new_prefix = sub_pref.replace(f"{bucket}/", "")
            logger.info(f". New prefix: {new_prefix}")
            s3.move_s3_file(bucket, sub_pref, new_prefix)
