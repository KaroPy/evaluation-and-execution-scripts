import pandas as pd


def label_cost_category(row):
    service = f"{row['serviceCategoryName']} {row['serviceName']}".lower()

    # Specific exclusion
    if "invalidations" in service:
        return "Other"

    # Storage-related services
    if any(
        keyword in service
        for keyword in [
            "storage",
            "s3",
            "volume",
            "snapshot",
            "bucket",
            "container registry - data stored",
            "backup",
        ]
    ):
        return "Storage"

    # Server/Compute-related services
    if any(
        keyword in service
        for keyword in [
            "compute",
            "ec2",
            "instance",
            "server",
            "virtual machine",
            "boxusage",
            "cpucredits",
            "spotusage",
            "ebsoptimized",
            "fargate",
            "lambda",
            "cluster",
        ]
    ):
        return "Servers"

    # Data transfer-related services
    if any(
        keyword in service
        for keyword in [
            "data transfer",
            "datatransfer",
            "bandwidth",
            "egress",
            "ingress",
            "in-bytes",
            "out-bytes",
            "bytes",
        ]
    ):
        return "Data Transfer"

    # Monitoring and metrics
    if any(
        keyword in service
        for keyword in [
            "dashboard",
            "monitor",
            "cw:metrics",
            "processingduration",
            "operation-count",
            "key vault",
            "log analytics",
            "network watcher",
        ]
    ):
        return "Monitoring"

    # API requests
    if "request" in service:
        return "API Requests"

    # Networking and infrastructure
    if any(
        keyword in service
        for keyword in [
            "load balancer",
            "natgateway",
            "ipv4",
            "lcuusage",
            "deliveryattempts",
            "application gateway",
            "azure dns",
            "microsoft.network",
        ]
    ):
        return "Networking"

    return "Other"


# Apply the labeling function
# TODO: Needs to be testeddf['Group'] = df.apply(label_cost_category, axis=1)
