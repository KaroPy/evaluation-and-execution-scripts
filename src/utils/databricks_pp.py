import numpy as np
import pandas as pd


def handle_databricks_cost(df: pd.DataFrame, cloud_name="databricks"):
    # add cluster_id
    df["serviceName"] = np.where(
        (df["cloud"] == cloud_name) & (df["cluster_id"].isnull() == False),
        df["cluster_id"],
        df["serviceName"],
    )
    df["serviceCategoryName"] = np.where(
        (df["cloud"] == cloud_name) & (df["cluster_id"].isnull() == False),
        df["resource_group"],
        df["serviceCategoryName"],
    )
    # add other resources
    df["serviceName"] = np.where(
        (df["cloud"] == cloud_name)
        & (df["cluster_id"].isnull())
        & (df["resource_name"].isnull() == False),
        df["resource_name"],
        df["serviceName"],
    )
    df["serviceCategoryName"] = np.where(
        (df["cloud"] == cloud_name)
        & (df["cluster_id"].isnull())
        & (df["resource_name"].isnull() == False),
        df["resource_group"],
        df["serviceCategoryName"],
    )
    # if resrource_name is null
    df["serviceName"] = np.where(
        (df["cloud"] == cloud_name)
        & (df["cluster_id"].isnull())
        & (df["resource_name"].isnull()),
        df["resource_group"],
        df["serviceName"],
    )
    df["serviceCategoryName"] = np.where(
        (df["cloud"] == cloud_name)
        & (df["cluster_id"].isnull())
        & (df["resource_name"].isnull()),
        df["resource_group"],
        df["serviceCategoryName"],
    )
    return df
