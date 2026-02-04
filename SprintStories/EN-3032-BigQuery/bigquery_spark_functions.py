"""
Spark functions for processing BigQuery conversion data and matching with profiles/views.
Transformed from first-insights-bigquery-spark-v1.ipynb
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, MapType
from typing import List, Optional


def preprocess_bigquery_data(
    bigquery_df: DataFrame,
    ids_for_profile_matching: List[str] = ["email_sha256"]
) -> DataFrame:
    """
    Preprocess BigQuery data: drop nulls, extract IDs from traits.

    Args:
        bigquery_df: Raw BigQuery events DataFrame with columns: _id, name, created, traits
        ids_for_profile_matching: List of trait keys to extract for profile matching

    Returns:
        Preprocessed DataFrame with extracted ID columns
    """
    # Drop rows without _id
    df = bigquery_df.filter(F.col("_id").isNotNull())

    # Select and rename columns
    df = df.select(
        F.col("_id").alias("bigquery_conversion_id"),
        F.col("name").alias("bigquery_name"),
        F.col("created").alias("bigquery_created"),
        F.col("traits")
    )

    # Parse traits JSON if it's a string, then extract IDs
    # Handle both string and map types for traits
    df = df.withColumn(
        "traits_parsed",
        F.when(
            F.col("traits").cast("string").isNotNull(),
            F.from_json(F.col("traits").cast("string"), MapType(StringType(), StringType()))
        ).otherwise(F.col("traits"))
    )

    # Extract each ID from traits
    for item in ids_for_profile_matching:
        df = df.withColumn(item, F.col("traits_parsed").getItem(item))

    # Drop intermediate columns
    df = df.drop("traits_parsed", "traits")

    return df


def extract_email_sha256_ids_udf():
    """
    Returns a UDF that extracts email_sha256 IDs from external_ids array.
    """
    @F.udf(ArrayType(StringType()))
    def extract_ids(external_ids):
        if external_ids is None:
            return []
        if isinstance(external_ids, str):
            import json
            try:
                external_ids = json.loads(external_ids)
            except:
                return []
        return [item['id'] for item in external_ids if item.get('name') == 'email_sha256']
    return extract_ids


def build_email_to_profile_mapping(profiles_df: DataFrame) -> DataFrame:
    """
    Build a mapping from email_sha256 to profile_id.

    Args:
        profiles_df: Profiles DataFrame with email_sha256_externalIds column

    Returns:
        DataFrame with columns: email_sha256, profile_id
    """
    extract_ids = extract_email_sha256_ids_udf()

    # Filter valid profiles and extract email IDs
    df = profiles_df.filter(F.col("email_sha256_externalIds").isNotNull())

    # Extract email IDs and explode
    df = df.withColumn("email_ids", extract_ids(F.col("email_sha256_externalIds")))
    df = df.select(
        F.col("profile_id"),
        F.explode("email_ids").alias("email_sha256")
    )

    return df


def match_bigquery_to_profiles(
    bigquery_df: DataFrame,
    profiles_df: DataFrame,
    email_column: str = "email_sha256"
) -> DataFrame:
    """
    Match BigQuery conversions to profiles using email_sha256.

    Args:
        bigquery_df: Preprocessed BigQuery DataFrame with email_sha256 column
        profiles_df: Profiles DataFrame with email_sha256_externalIds and anonymousId
        email_column: Column name for email matching

    Returns:
        BigQuery DataFrame enriched with profile_id and anonymousId
    """
    # Build email to profile mapping
    email_mapping = build_email_to_profile_mapping(profiles_df)

    # Join bigquery with email mapping
    df = bigquery_df.join(
        email_mapping,
        on=email_column,
        how="left"
    )

    # Join with profiles to get anonymousId
    df = df.join(
        profiles_df.select("profile_id", "anonymousId"),
        on="profile_id",
        how="left"
    )

    # Select final columns
    df = df.select(
        "profile_id",
        "anonymousId",
        "bigquery_name",
        "bigquery_created",
        "bigquery_conversion_id"
    )

    return df


def calculate_match_statistics(bigquery_df: DataFrame) -> dict:
    """
    Calculate matching statistics for BigQuery conversions.

    Args:
        bigquery_df: DataFrame with profile_id column (after matching)

    Returns:
        Dictionary with match statistics
    """
    total = bigquery_df.select("bigquery_conversion_id").distinct().count()

    matched = bigquery_df.filter(
        F.col("profile_id").isNotNull()
    ).select("bigquery_conversion_id").distinct().count()

    unmatched = total - matched
    match_percentage = (matched / total * 100) if total > 0 else 0

    return {
        "total_conversion_ids": total,
        "matched_conversion_ids": matched,
        "unmatched_conversion_ids": unmatched,
        "match_percentage": match_percentage
    }


def deduplicate_bigquery_matches(bigquery_df: DataFrame) -> DataFrame:
    """
    Deduplicate BigQuery matches, keeping first occurrence per anonymousId/conversion.

    Args:
        bigquery_df: Matched BigQuery DataFrame

    Returns:
        Deduplicated DataFrame
    """
    # Filter to matched profiles only
    df = bigquery_df.filter(F.col("anonymousId").isNotNull())

    # Deduplicate by anonymousId and bigquery_conversion_id
    df = df.dropDuplicates(["anonymousId", "bigquery_conversion_id"])

    return df


def calculate_session_ranges(views_df: DataFrame) -> DataFrame:
    """
    Calculate session start and end times per anonymousId and session.

    Args:
        views_df: Views DataFrame with session, created, anonymousId columns

    Returns:
        DataFrame with session ranges: anonymousId, session, session_start, session_end
    """
    return views_df.groupBy("anonymousId", "session").agg(
        F.min("created").alias("session_start"),
        F.max("created").alias("session_end")
    )


def merge_bigquery_with_sessions(
    bigquery_df: DataFrame,
    views_df: DataFrame,
    buffer_minutes: int = 60
) -> DataFrame:
    """
    Merge BigQuery conversions with sessions based on time overlap.

    Args:
        bigquery_df: Deduplicated BigQuery DataFrame with anonymousId
        views_df: Views DataFrame
        buffer_minutes: Time buffer (minutes) for matching conversions to sessions

    Returns:
        Merged DataFrame with session information
    """
    # Ensure datetime types
    bigquery_df = bigquery_df.withColumn(
        "bigquery_created",
        F.to_timestamp("bigquery_created")
    )
    views_df = views_df.withColumn(
        "created",
        F.to_timestamp("created")
    )

    # Calculate session ranges
    session_ranges = calculate_session_ranges(views_df)

    # Join on anonymousId
    merged = bigquery_df.join(
        session_ranges,
        on="anonymousId",
        how="inner"
    )

    # Filter by time range with buffer
    buffer = F.expr(f"INTERVAL {buffer_minutes} MINUTES")
    merged = merged.filter(
        (F.col("bigquery_created") >= F.col("session_start") - buffer) &
        (F.col("bigquery_created") <= F.col("session_end") + buffer)
    )

    return merged


def enrich_views_with_bigquery(
    views_df: DataFrame,
    matched_sessions_df: DataFrame,
    conversion_name: str = "checkout_completed"
) -> DataFrame:
    """
    Enrich views with BigQuery conversion information.

    Args:
        views_df: Original views DataFrame
        matched_sessions_df: DataFrame with matched session/conversion info
        conversion_name: Name to assign for matched conversions with null conv_name

    Returns:
        Enriched views DataFrame
    """
    # Select join columns from matched sessions
    session_conversions = matched_sessions_df.select(
        "anonymousId",
        "session",
        "bigquery_name",
        "bigquery_conversion_id"
    ).dropDuplicates(["anonymousId", "session"])

    # Left join views with session conversions
    enriched = views_df.join(
        session_conversions,
        on=["anonymousId", "session"],
        how="left"
    )

    # Store original conv_name
    enriched = enriched.withColumn("conv_name_original", F.col("conv_name"))

    # Update conv_name where bigquery_name exists but conv_name is null
    enriched = enriched.withColumn(
        "conv_name",
        F.when(
            (F.col("bigquery_name").isNotNull()) & (F.col("conv_name").isNull()),
            F.lit(conversion_name)
        ).otherwise(F.col("conv_name"))
    )

    return enriched


def process_bigquery_pipeline(
    bigquery_df: DataFrame,
    profiles_df: DataFrame,
    views_df: DataFrame,
    ids_for_profile_matching: List[str] = ["email_sha256"],
    buffer_minutes: int = 60,
    conversion_name: str = "checkout_completed"
) -> tuple[DataFrame, dict]:
    """
    Full pipeline to process BigQuery data and enrich views.

    Args:
        bigquery_df: Raw BigQuery events DataFrame
        profiles_df: Profiles DataFrame
        views_df: Views DataFrame
        ids_for_profile_matching: List of trait keys for profile matching
        buffer_minutes: Time buffer for session matching
        conversion_name: Name for matched conversions

    Returns:
        Tuple of (enriched_views_df, match_statistics)
    """
    # Step 1: Preprocess BigQuery data
    bigquery_processed = preprocess_bigquery_data(
        bigquery_df,
        ids_for_profile_matching
    )

    # Step 2: Match to profiles
    bigquery_matched = match_bigquery_to_profiles(
        bigquery_processed,
        profiles_df
    )

    # Step 3: Calculate statistics
    stats = calculate_match_statistics(bigquery_matched)

    # Step 4: Deduplicate matches
    bigquery_deduped = deduplicate_bigquery_matches(bigquery_matched)

    # Step 5: Merge with sessions
    session_merged = merge_bigquery_with_sessions(
        bigquery_deduped,
        views_df,
        buffer_minutes
    )

    # Step 6: Enrich views
    enriched_views = enrich_views_with_bigquery(
        views_df,
        session_merged,
        conversion_name
    )

    return enriched_views, stats


# ============== Data Loading Functions ==============

def load_bigquery_from_s3(
    spark: SparkSession,
    workspace_id: str,
    bucket_prefix: str = "bigQueryEvents/"
) -> DataFrame:
    """
    Load BigQuery events from S3.

    Args:
        spark: SparkSession
        workspace_id: Workspace ID (S3 bucket name)
        bucket_prefix: Prefix path in bucket

    Returns:
        BigQuery events DataFrame
    """
    path = f"s3://{workspace_id}/{bucket_prefix}*.json"
    return spark.read.json(path)


def load_delta_share_table(
    spark: SparkSession,
    share_path: str,
    table_name: str
) -> DataFrame:
    """
    Load a table from Delta Sharing.

    Args:
        spark: SparkSession
        share_path: Path to share profile
        table_name: Full table name (e.g., delta_share_events.workspace_id.table_name)

    Returns:
        DataFrame from Delta Share
    """
    return spark.read.format("deltaSharing").load(f"{share_path}#{table_name}")
