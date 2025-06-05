from datetime import datetime
import pandas as pd
from src.extractors.stackit_extractory import StackItExtractor
from src.extractors.prefect_github_deployment_extractory import (
    ExtracPrefectGithubDeployments,
)
from src.extractors.prefect_runtime_extractor import PrefectRuntimeExtractor
from src.extractors.azure_extractor import AzureExtractor
from src.extractors.aws_extractor import AWSExtractor
from src.extractors.databricks_extractor import DatabricksUsageExtractor
from src.utils.logging_definitions import get_logger
from src.utils.directory_handling import create_directory_if_not_exists
from src.data_combination.data_combination import (
    combine_deployments_and_prefect_runs,
    combine_prefect_runs_with_deployments_and_costs,
    concat_two_dataframes,
)

timestamp = datetime.now()
logger = get_logger(f"extract_overview_of_costs_{timestamp}_v3_databricks")
from_date = "2024-01-01"
to_date = "2025-06-05"
path_to_save = f"data/{from_date}_to_{to_date}/{timestamp}/"
create_directory_if_not_exists(f"{path_to_save}")
logger.info(f"Extracting costs for date range: {from_date} to {to_date}")

# get stackit costs
stackit = StackItExtractor(logger, path_to_save=path_to_save)
stackit_costs = stackit.extract_costs(from_date, to_date)
stackit_costs.to_csv(
    f"{path_to_save}stackit_costs_merged_{from_date}_{to_date}.csv", index=False
)

# get prefect deployments
prefect_deployments = ExtracPrefectGithubDeployments(logger=logger)
json_data = prefect_deployments.extract_prefect_deployment_specifications(
    save_deployments=True, load_existing_data=True
)
deployment_specs = prefect_deployments.combine_prefect_deployment_specifications(
    json_data, load_existing_data=True
)

# get prefect runs
prefect = PrefectRuntimeExtractor(logger=logger, path_save=path_to_save)
df_prefect_runs = prefect.extract_runtimes(
    from_date=from_date, to_date=to_date, save_data=True
)

# merge data and stackit costs
# merge prefect runs with deployment specs
prefect_runs_with_deployments = combine_deployments_and_prefect_runs(
    df_prefect_runs, deployment_specs
)
prefect_runs_with_deployments.to_csv(
    f"{path_to_save}prefect_runs_with_deployments_{from_date}_{to_date}.csv"
)

final_costs = combine_prefect_runs_with_deployments_and_costs(
    prefect_runs_with_deployments, stackit_costs
)
final_costs.to_csv(f"{path_to_save}final_costs_stackit_{from_date}_{to_date}.csv")

# add azure costs
azure_client = AzureExtractor(logger=logger, path_to_save=path_to_save)
azure_costs = azure_client.get_costs(
    from_date=from_date, to_date=to_date, save_data=True
)
logger.info(f"final_costs: {final_costs.shape}")
logger.info(f"azure_costs: {azure_costs.shape}")
final_costs_with_azure = concat_two_dataframes(final_costs, azure_costs)
final_costs_with_azure.to_csv(
    f"{path_to_save}final_costs_with_azure_{from_date}_{to_date}.csv"
)
final_costs_with_azure = pd.read_csv(
    f"{path_to_save}final_costs_with_azure_{from_date}_{to_date}.csv"
)
logger.info(f"final_costs_with_azure: {final_costs_with_azure.shape}")

# add aws costs
aws_client = AWSExtractor(logger=logger, path_to_save=path_to_save)
aws_costs = aws_client.extract_costs(
    from_date=from_date, to_date=to_date, save_data=True
)

final_costs_with_azure_and_aws = concat_two_dataframes(
    final_costs_with_azure, aws_costs
)
final_costs_with_azure_and_aws.to_csv(
    f"{path_to_save}final_costs_with_azure_and_aws_{from_date}_{to_date}.csv"
)

final_costs_with_azure_and_aws = pd.read_csv(
    f"{path_to_save}final_costs_with_azure_and_aws_{from_date}_{to_date}.csv"
)

databricks_client = DatabricksUsageExtractor(logger=logger, path_to_save=path_to_save)
databricks_usage = databricks_client.load_cost(
    from_date=from_date, to_date=to_date, save_data=True
)
databricks_usage.to_csv(f"{path_to_save}databricks_usage_{from_date}_{to_date}.csv")

final_costs_with_azure_and_aws_and_db = concat_two_dataframes(
    final_costs_with_azure_and_aws, databricks_usage
)
final_costs_with_azure_and_aws_and_db.to_csv(
    f"{path_to_save}final_costs_with_azure_and_aws_and_db_{from_date}_{to_date}.csv"
)
