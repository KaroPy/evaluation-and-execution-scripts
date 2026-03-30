import boto3
import pandas as pd

"""
DOES NOT WORK - MUST BE ENABLED FOR BUCKETS
"""


def return_period(from_date, to_date):
    nr_days = (to_date - from_date).days
    if nr_days < 15:
        # Start time between 3 hours and 15 days ago - Use a multiple of 60 seconds (1 minute).
        return 60 * 60 * 24 * nr_days
    elif nr_days < 63:
        # Start time between 15 and 63 days ago - Use a multiple of 300 seconds (5 minutes).
        return 300 * 24 * nr_days
    else:
        # Start time greater than 63 days ago - Use a multiple of 3600 seconds (1 hour).
        return 3600 * 24 * nr_days


def query_bucket_downloads(
    bucket_name: str,
    from_date: str,
    to_date: str,
    storage_type="StandardStorage",
):
    cloudwatch = boto3.client("cloudwatch")
    from_date = pd.to_datetime(from_date)
    to_date = pd.to_datetime(to_date)

    period = return_period(from_date, to_date)

    # BytesDownloaded requires S3 request metrics enabled on the bucket.
    # Enable in S3 console: Bucket → Metrics → Request metrics → Create filter.
    # Use FilterId="EntireBucket" if configured for the whole bucket.
    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/S3",
        MetricName="BytesDownloaded",
        Dimensions=[
            {"Name": "BucketName", "Value": bucket_name},
            {"Name": "StorageType", "Value": storage_type},
        ],
        StartTime=from_date,
        EndTime=to_date,
        Period=period,
        Statistics=["Sum"],
    )
    print(response)
    datapoints = response.get("Datapoints", [])
    if not datapoints:
        print(
            "No data available. Note: It may take up to 24h for CloudWatch to show bucket size."
        )
        return None

    # Return the most recent data point
    latest = max(datapoints, key=lambda x: x["Timestamp"])
    return latest["Average"]


def main():
    bucket_name = "603f64f26e64740031116698-databricks"
    from_date = "2026-03-03"
    to_date = "2026-03-05"
    storage_type = "StandardStorage"
    query_bucket_downloads(bucket_name, from_date, to_date, storage_type)


if __name__ == "__main__":
    main()
