import json
import logging
import pandas as pd

from src.configs.prefect_specs import (
    DeploymentType,
    RETRAINING_DEPLOYMENT,
    RETRAINING_NODE_NAME_KEY,
    RETRAINING_NODE_NAME_KEY_RESET,
    TARGETING_DEPLOYMENT,
)


def transform_dict_to_pandas_df(json_data: json, logger: logging, dict_type=None):
    if dict_type is None:
        return pd.json_normalize(json_data)
    elif dict_type == DeploymentType.ETLFLOW.value:
        logger.info(f"Transform ETLFlow")
        df = transform_etl_flow_dict(json_data)
        return df
    elif dict_type == DeploymentType.RETRAINING.value:
        logger.info(f"Transform Retraining")
        df = transform_retraining_dict(json_data)
        return df
    elif dict_type == DeploymentType.TARGETING.value:
        logger.info(f"Transform Targeting")
        df = transform_targeting_dict(json_data)
        return df
    else:
        raise Exception(f"Type {dict_type} is not defined")


def transform_etl_flow_dict(new_dict):
    rows = []
    for deployment, data in new_dict.items():
        deployment_config = data.get("deployment_config", [{}])
        for config in deployment_config:
            job_vars = config.get("job_variables", {})
            rows.append(
                {
                    "Deployments": deployment,
                    "tenant": config.get("tenant"),
                    "node_name": job_vars.get("node_name"),
                }
            )
    return pd.DataFrame(rows)


def transform_retraining_dict(dict: json):
    rows = []
    for tenant, data in dict.items():
        audiences = data.get("retraining-audiences", [{}])
        for audience in audiences:
            rows.append(
                {
                    "Deployments": RETRAINING_DEPLOYMENT,
                    "tenant": tenant,
                    "audience": audience.get("audience"),
                    "schedule": audience.get("schedule"),
                    "max_model_age_in_days": audience.get("max_model_age_in_days"),
                    "node_name": data.get(RETRAINING_NODE_NAME_KEY),
                    "node_retraining_reset": data.get(
                        RETRAINING_NODE_NAME_KEY_RESET, None
                    ),
                }
            )
    return pd.DataFrame(rows)


def transform_targeting_dict(dict: json):
    rows = []
    for tenant, data in dict.items():
        for audience in data:
            rows.append(
                {
                    "Deployments": TARGETING_DEPLOYMENT,
                    "tenant": tenant,
                    "audience": audience.get("audience"),
                    "schedule": audience.get("schedule"),
                    "use_conversion_table_targeting": audience.get(
                        "use_conversion_table_targeting"
                    ),
                }
            )
    return pd.DataFrame(rows)
