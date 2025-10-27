import os
import boto3
from datetime import datetime, timedelta


class S3Connection:
    """
    Class to connect to a S3 bucket and list its files
    """

    def __init__(self):
        """
        Initialize the class with the bucket name and AWS keys
        """
        aws_key = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
        )

    # def tag_bucket(self, bucket_name, tags: dict):
    #     tagging_dict = {"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]}
    #     self.s3.put_bucket_tagging(
    #         Bucket=bucket_name,
    #         Tagging=tagging_dict,
    #     )
    def download_file(self, bucket_name: str, file_name: str, local_path: str):
        self.s3.download_file(bucket_name, file_name, local_path)

    def list_buckets(self):
        buckets = self.s3.list_buckets()
        buckets = [bucket["Name"] for bucket in buckets["Buckets"]]
        return buckets

    def copy_all_files_recursively(
        self, bucket_name: str, source_prefix: str, dest_prefix: str
    ):
        """Copy all files from source_prefix to dest_prefix recursively"""
        print(f"Copying all files from {source_prefix} to {dest_prefix}")

        # Use paginator to handle large number of files
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)

        for page in pages:
            if "Contents" not in page:
                print(f"No files found in {source_prefix}")
                continue

            for obj in page["Contents"]:
                source_key = obj["Key"]

                # Skip if it's just the prefix itself (folder)
                if source_key == source_prefix:
                    continue

                # Calculate destination key by replacing source prefix with dest prefix
                relative_path = source_key[len(source_prefix) :].lstrip("/")
                dest_key = f"{dest_prefix}/{relative_path}"

                # Use your copy_s3_file method
                self.copy_s3_file(bucket_name, source_key, dest_key)

    def copy_s3_file(self, bucket_name: str, prefix: str, new_prefix: str):
        # print(f"Copying {prefix} to {new_prefix}")
        self.s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": prefix},
            Key=new_prefix,
        )

    def move_s3_file(self, bucket_name: str, prefix: str, new_prefix: str):
        print(f"Moving {prefix} to {new_prefix}")
        self.s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": prefix},
            Key=new_prefix,
        )
        self.s3.delete_object(Bucket=bucket_name, Key=prefix)

    def list_files_with_pagination(self, bucket_name: str, prefix: str, delimiter=None):
        """
        List all files in the S3 bucket
        """
        files = []
        kwargs = {"Bucket": bucket_name, "Prefix": prefix}

        if delimiter is not None:
            kwargs["Delimiter"] = delimiter

        while True:
            response = self.s3.list_objects_v2(**kwargs)
            if "Contents" in response.keys():
                files.extend([obj["Key"] for obj in response["Contents"]])
            if "CommonPrefixes" in response.keys():
                files.extend([obj["Prefix"] for obj in response["CommonPrefixes"]])
            try:
                kwargs["ContinuationToken"] = response["NextContinuationToken"]
            except KeyError:
                break

        return files

    def list_files(self, bucket_name: str, prefix: str, delimiter=None):
        """
        List all files in the S3 bucket
        """
        if delimiter is None:
            response = self.s3.list_objects(Bucket=bucket_name, Prefix=prefix)
            if "Contents" not in response.keys():
                print("No files found")
                return []
            files = [obj["Key"] for obj in response["Contents"]]
        else:
            response = self.s3.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, Delimiter="/"
            )
            if "CommonPrefixes" not in response.keys():
                print("No files found")
                return []
            files = [obj["Prefix"] for obj in response["CommonPrefixes"]]
        return files

    def get_s3_bucket_size_cloudwatch(
        self, bucket_name, storage_type="StandardStorage"
    ):
        cloudwatch = boto3.client("cloudwatch")

        response = cloudwatch.get_metric_statistics(
            Namespace="AWS/S3",
            MetricName="BucketSizeBytes",
            Dimensions=[
                {"Name": "BucketName", "Value": bucket_name},
                {"Name": "StorageType", "Value": storage_type},
            ],
            StartTime=datetime.now() - timedelta(days=2),
            EndTime=datetime.now(),
            Period=86400,
            Statistics=["Average"],
        )
        datapoints = response.get("Datapoints", [])
        if not datapoints:
            print(
                "No data available. Note: It may take up to 24h for CloudWatch to show bucket size."
            )
            return None

        # Return the most recent data point
        latest = max(datapoints, key=lambda x: x["Timestamp"])
        return latest["Average"]

    def get_prefix_size(self, bucket_name, prefix):
        paginator = self.s3.get_paginator("list_objects_v2")
        total_size = 0

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                total_size += obj["Size"]

        return total_size

    def read_json_from_aws(self, bucket_name, file_name):
        response = self.s3.get_object(Bucket=bucket_name, Key=file_name)
        data = response["Body"].read().decode("utf-8")
        data = data.rstrip("\x00")  # remove trailing null bytes
        return data

    def list_all_prefixes(self, bucket_name, prefix=""):
        response = self.s3.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        )
        prefixes = [obj["Prefix"] for obj in response.get("CommonPrefixes", [])]
        for prefix in prefixes:
            yield prefix
            yield from self.list_all_prefixes(bucket_name, prefix)

    def tag_bucket(self, bucket_name, key_values: list):
        self.s3.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={"TagSet": key_values},  # [{"Key": "account", "Value": "test"}]},
        )
