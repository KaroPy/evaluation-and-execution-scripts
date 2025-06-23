import pandas as pd


def stack_pp(data: pd.DataFrame):
    """
    Function to handle special cases for stackit data
    """
    cases = {
        "serviceName": {
            "General Purpose Server-g1.4-EU01": {
                "node_name": "testing-server-karo"  # e.g. innkeepr-analytics-2: deleted on 20.06.2025
            },  # e.g. innkeepr-analytics-2: deleted on 20.06.2025
            "General Purpose Server-g1.1-EU01": {
                "node_name": "testing-server-karo"
            },  # analytics-testing-server: deleted on 20.06.2025
            "Compute Optimized Server-c1.2-EU01": {"node_name": "postgres-prefect"},
            # "Purpose Server-g1.3-EU01": {
            #     "agentpooleu01-2": {
            #         "duration": 86400,
            #     },  # in seconds (24hrs)
            #     "agentpooleu01-3": {
            #         "duration": 86400,
            #     },  # in seconds (24hrs)
            # },
            "Tiny Server-t1.2-EU01": {"node_name": "other-stackit-server"},
        }
    }
    for label in cases.keys():
        if label == "serviceName":
            data = handle_adaption_of_services(data, cases[label])
        else:
            raise ValueError(f"The label {label} is not defined")
    return data


def handle_adaption_of_services(data: pd.DataFrame, cases: dict):
    col_prefect_deployment = "Prefect_Deployments"  # TODO add as colname to configs
    col_service_name = "serviceName"  # TODO add as colname to configs
    col_node_name = "node_name"  # TODO add as colname to configs
    col_duration = "duration"  # TODO add as colname to configs
    for service in cases:
        print(f"Service: {service}")
        idx = data[data[col_service_name] == service].index
        data.loc[idx, col_prefect_deployment] = "stackit_default_servers"
        if service == "Purpose Server-g1.3-EU01":
            for agent in cases[service]:
                idx = len(data)
                data.loc[idx, col_node_name] = agent
                data.loc[idx, col_duration] = cases[service][agent]["duration"]
        else:
            data.loc[idx, col_node_name] = cases[service][col_node_name]
    return data
