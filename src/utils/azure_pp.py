import pandas as pd
import numpy as np


def handle_azure_costs(
    df: pd.DataFrame,
    cloud_name="azure",
    azure_vms_relation_to_github={
        "4cpu8gib": "D4ls v5",  # 4vCPUs, 8 GiB RAM, 0.18€/h
        "4cpu16gib": "D4 v3/D4s v3",  # D4s_v3 4vCPUs, 16 GiB RAM, 0.22€/h
        "4cpu32gib": "D8 v3/D8s v3",  # D8s_v3, 8vCPUs, 32 GiB RAM, 0.45€/h
        "4cpu64gib": "D16s v4",  # Standard_D16s_v4
        "8cpu128gib": "E16as v5",  # E16as_v5, 16vCPUs, 128 GiB RAM, 1.03€/h
        # "8cpu128gib": "E16-4ads v5", #nur ab dem 10.01 davor E32as v5
        "16cpu256gib": "E32as v5",  # E32as_v5, 32vCPUs, 256 GiB RAM, 2.05€/h
        # "16cpu256gib": "E32-8ads v5", #nur ab dem 10.01 davor E32as v5
        "30cpu476gib": "E48a sv5",  # 38vCPUs, 384 GiB RAM, 3.07€/h
    },
):
    df["machine.type"] = np.where(
        (df["cloud"] == cloud_name)
        & (df["machine.type"].isnull())
        & (df["serviceName"].isnull() == False),
        df["serviceName"]
        .str.split(" - ")
        .str[1],  # Virtual Machines Dlsv5 Series - D4ls v5 - EU West
        df["machine.type"],
    )
    df["machine.type"] = np.where(
        df["machine.type"] == "E16-4ads v5", "E16as v5", df["machine.type"]
    )
    df["machine.type"] = np.where(
        df["machine.type"] == "E32-8ads v5", "E32as v5", df["machine.type"]
    )
    azure_vms_relation_to_github_pd = pd.DataFrame(
        data={
            "node_name": azure_vms_relation_to_github.keys(),
            "machine.type": azure_vms_relation_to_github.values(),
        }
    )
    df = pd.merge(
        df,
        azure_vms_relation_to_github_pd,
        on="machine.type",
        how="left",
        suffixes=("", "_azure"),
    )
    df["node_name"] = np.where(
        (df["node_name"].isnull()) & (df["cloud"] == cloud_name),
        df["node_name_azure"],
        df["node_name"],
    )
    df = df.drop(columns=["node_name_azure"])
    return df
