import pandas as pd
import numpy as np


def handle_aws_costs(df: pd.DataFrame, cloud_name="aws"):
    return df
    # TODO: not necessary anymore
    df["servicename_aws"] = None
    df["servicename_aws"] = np.where(
        (df["cloud"] == cloud_name) & (df["serviceName"].isnull() == False),
        df["serviceName"],
        df["servicename_aws"],
    )
    df["serviceName"] = np.where(
        (df["cloud"] == cloud_name) & (df["serviceCategoryName"].isnull() == False),
        df["serviceCategoryName"],
        df["serviceName"],
    )
    df["serviceCategoryName"] = df["servicename_aws"]
    return df
