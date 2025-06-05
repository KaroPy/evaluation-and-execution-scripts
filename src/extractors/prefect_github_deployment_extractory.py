import json
import pandas as pd
import numpy as np
from src.utils.yaml_handling import read_yaml_file
from src.utils.github_connection import get_github_directories, read_yaml_from_github
from src.utils.json_handling import write_data_to_json, read_data_from_json
from src.utils.constants import return_repo_name_etlflow, return_repo_name_targeting
from src.utils.prefect_data_handling import transform_dict_to_pandas_df
from src.configs.prefect_specs import (
    NoDeploymentFileFound,
    DeploymentSpec,
    RepositoryType,
    DeploymentType,
    MESSAGE_NO_DEPLOYMENT_FILE_FOUND,
    RETRAINING_NODE_SPECIFICATIONS,
    RETRAINING_DEPLOYMENT,
    RETRAINING_NODE_NAME_KEY,
    RETRAINING_NODE_NAME_KEY_RESET,
    TARGETING_DEPLOYMENT,
    TARGETING_NODE_MAPPING_KEY,
    ETLFLOW_DEPLOYMENT_CONFIG_KEY,
)


class CleanDataDeployments:
    def __init__(self, json_data: json, logger):
        self.logger = logger
        self.json_data = json_data

    def clean_json_data(self, repository):
        keys_to_remove = []
        if repository == RepositoryType.ETLFLOWS.value:
            keep_specification = ["job_variables", "tenant"]
            json_keys = self.json_data[ETLFLOW_DEPLOYMENT_CONFIG_KEY][0].keys()
            for json_key in json_keys:
                if json_key not in keep_specification:
                    keys_to_remove.append(json_key)
            for json_key in keys_to_remove:
                self.json_data[ETLFLOW_DEPLOYMENT_CONFIG_KEY][0].pop(json_key)
        return self.json_data


class ExtracPrefectGithubDeployments:
    """
    Class to extract prefect relevant flow information for cost monitoring
    """

    def __init__(self, logger):
        self.logger = logger
        self.configs = read_yaml_file("src/configs/prefect-deployments.yaml")
        print(f"configs = {self.configs}")
        self.saving_dir = "data/"

    def extract_prefect_deployment_specifications(
        self, save_deployments=False, load_existing_data=False
    ):
        """
        Extract all prefect deployment specifications using the configurations
        form configs
        Args:
            save_deployments (bool): if True, then it will save it to a json file
        Returns:
            prefect_deployments: all relevant prefect deployments with specifications
        """
        if load_existing_data:
            self.logger.info("extract_prefect_deployment_specifications: load data")
            prefect_deployments = read_data_from_json(
                f"{self.saving_dir}prefect_deployments_final",
            )
            return prefect_deployments

        prefect_deployments = {}
        configs = self.configs["prefect-deployments"]
        for key in configs.keys():
            if "repository" in key:
                repository = configs[key]["name"]
                path_to_deployments = configs[key]["path_to_deployments"]
                deployment_file = configs[key]["deployment_file"]
                deployments_to_ignore = configs[key]["deployment_to_ignore"]
                if repository not in prefect_deployments.keys():
                    prefect_deployments[repository] = {}
                self.logger.info(
                    f"repository: {repository}, path_to_deployments: {path_to_deployments}"
                )

                if path_to_deployments == DeploymentSpec.KUBERNETES.value:
                    list_dirs = [""]
                else:
                    list_dirs = get_github_directories(
                        repo=repository, path=path_to_deployments, logger=self.logger
                    )
                list_dirs = [
                    directory
                    for directory in list_dirs
                    if directory not in deployments_to_ignore
                ]
                self.logger.info(f"list_dirs = {list_dirs}")
                for directory in list_dirs:
                    self.logger.info(f"{repository}: directory = {directory}")
                    dir_path = f"{path_to_deployments}/{directory}"
                    deployment_content = read_yaml_from_github(
                        repository, dir_path, deployment_file, self.logger
                    )
                    clean_data = CleanDataDeployments(
                        json_data=deployment_content, logger=self.logger
                    )
                    deployment_content = clean_data.clean_json_data(
                        repository=repository
                    )
                    if path_to_deployments == DeploymentSpec.KUBERNETES.value:
                        directory = RETRAINING_NODE_SPECIFICATIONS
                    prefect_deployments[repository][directory] = deployment_content
                    self.logger.info(
                        f"prefect_deployments: {prefect_deployments.keys()}"
                    )
                    self.logger.info("-----------------------------")
                    if save_deployments:
                        write_data_to_json(
                            prefect_deployments,
                            f"{self.saving_dir}prefect_deployments_temp",
                        )
                    if len(prefect_deployments) == 0:
                        if len(deployment_content) == 0:
                            raise NoDeploymentFileFound(
                                f"{MESSAGE_NO_DEPLOYMENT_FILE_FOUND}: {dir_path}"
                            )
        if save_deployments:
            write_data_to_json(
                prefect_deployments,
                f"{self.saving_dir}prefect_deployments_final",
            )
        return prefect_deployments

    def return_list_of_keys(self, json_data: json, ignore_value=list | None):
        list_of_keys = list(json_data.keys())
        if ignore_value is not None:
            list_of_keys = [
                tenant for tenant in list_of_keys if tenant not in ignore_value
            ]
        return list_of_keys

    def combine_k8s_retraining_with_nodes(self, json_data: json, save_data=False):
        dict_retraining_specs = {}
        repository = return_repo_name_targeting()
        list_tenants = self.return_list_of_keys(
            json_data=json_data[repository],
            ignore_value=[RETRAINING_NODE_SPECIFICATIONS],
        )
        self.logger.info(f"combine_k8s_retraining_with_nodes tenants: {list_tenants}")
        json_data_nodes = json_data[repository][RETRAINING_NODE_SPECIFICATIONS][
            TARGETING_NODE_MAPPING_KEY
        ]
        for tenant in list_tenants:
            # self.logger.info(f"Get retraining specs for {tenant}")
            dict_retraining_specs[tenant] = {}
            if tenant in list(json_data_nodes.keys()):
                json_data_tenant = json_data_nodes[tenant]
                node_size = json_data_tenant[RETRAINING_NODE_NAME_KEY]
                dict_retraining_specs[tenant][RETRAINING_NODE_NAME_KEY] = node_size
                if RETRAINING_NODE_NAME_KEY_RESET in json_data_tenant.keys():
                    node_size_reset = json_data_tenant[RETRAINING_NODE_NAME_KEY_RESET]
                    dict_retraining_specs[tenant][
                        RETRAINING_NODE_NAME_KEY_RESET
                    ] = node_size_reset
            # add audience data
            if RETRAINING_DEPLOYMENT in json_data[repository][tenant].keys():
                audiences = json_data[repository][tenant][RETRAINING_DEPLOYMENT]
                if len(audiences) > 0:
                    dict_retraining_specs[tenant][
                        f"{RETRAINING_DEPLOYMENT}-audiences"
                    ] = audiences
        if save_data:
            write_data_to_json(
                dict_retraining_specs, f"{self.saving_dir}dict_retraining_specs"
            )
        return dict_retraining_specs

    def get_dict_k8s_targeting(self, json_data, save_data=False):
        repository = return_repo_name_targeting()
        dict_targeting = {}
        list_tenants = self.return_list_of_keys(
            json_data=json_data[repository],
            ignore_value=[RETRAINING_NODE_SPECIFICATIONS],
        )
        self.logger.info(f"get_dict_k8s_targeting tenants: {list_tenants}")
        for tenant in list_tenants:
            # self.logger.info(f"tenant = {tenant}")
            json_tenant = json_data[repository][tenant]
            if TARGETING_DEPLOYMENT in json_tenant.keys():
                dict_targeting[tenant] = json_tenant[TARGETING_DEPLOYMENT]

        if save_data:
            write_data_to_json(dict_targeting, f"{self.saving_dir}dict_targeting")
        return dict_targeting

    def get_dict_for_etl_flows(self, json_data, save_data=False):
        repository = return_repo_name_etlflow()
        dict_etl_specs = json_data[repository]
        if save_data:
            write_data_to_json(dict_etl_specs, f"{self.saving_dir}dict_etl_specs")
        return dict_etl_specs

    def clean_data(self, data: pd.DataFrame):
        use_deployments = [
            "etlFlow",
            "monitorActiveEtlRuns",
            "monitorInfrastructureExpirations",
            "monitorStackitCosts",
            "trainingReadyCheck",
            "targeting",
            "retraining",
        ]
        data = data[data["Deployments"].isin(use_deployments)]
        data["node_name"] = np.where(
            (data["node_name"].isnull()) & (data["Deployments"] == "etlFlow"),
            "2cpu4gib",  # TODO: insert from prefect flows?
            data["node_name"],
        )
        return data

    def combine_prefect_deployment_specifications(
        self, json_data: json, load_existing_data=False, save_data=True
    ):
        if load_existing_data:
            self.logger.info("combine_prefect_deployment_specifications: load data")
            all_data = pd.read_csv(
                f"{self.saving_dir}prefect_deployment_specifications_df.csv"
            )
            return all_data
        dict_retraining_specs = self.combine_k8s_retraining_with_nodes(
            json_data, save_data=save_data
        )

        dict_targeting_specs = self.get_dict_k8s_targeting(
            json_data, save_data=save_data
        )
        # TODO: match targeting nodes and targeting audiences via prefect logs
        dict_etlflow_specs = self.get_dict_for_etl_flows(json_data, save_data=save_data)

        df_retraining_specs = transform_dict_to_pandas_df(
            dict_retraining_specs, self.logger, DeploymentType.RETRAINING.value
        )
        df_targeting_specs = transform_dict_to_pandas_df(
            dict_targeting_specs, self.logger, DeploymentType.TARGETING.value
        )
        df_etlflow_specs = transform_dict_to_pandas_df(
            dict_etlflow_specs, self.logger, DeploymentType.ETLFLOW.value
        )
        all_data = pd.concat(
            [df_etlflow_specs, df_targeting_specs, df_retraining_specs]
        )
        all_data = self.clean_data(all_data)
        if save_data:
            all_data.to_csv(
                f"{self.saving_dir}prefect_deployment_specifications_df.csv"
            )
        return all_data
