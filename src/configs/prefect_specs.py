from enum import Enum

MESSAGE_NO_DEPLOYMENT_FILE_FOUND = "No deployment file was found in: "
RETRAINING_NODE_SPECIFICATIONS = "k8s-nodes"
RETRAINING_DEPLOYMENT = "retraining"
RETRAINING_NODE_NAME_KEY = f"k8-{RETRAINING_DEPLOYMENT}"
RETRAINING_NODE_NAME_KEY_RESET = f"k8-{RETRAINING_DEPLOYMENT}-reset"
TARGETING_DEPLOYMENT = "targeting"
TARGETING_NODE_MAPPING_KEY = "node_size_mapping"
ETLFLOW_DEPLOYMENT_CONFIG_KEY = "deployment_config"


class NoDeploymentFileFound(Exception):
    """Exception raised when no deployment file is found in the repository."""

    pass


class DeploymentSpec(Enum):
    KUBERNETES = "config/kubernetes"
    OTHER = "other"


class RepositoryType(Enum):
    ETLFLOWS = "prefect-flows-etl"
    OTHER = "other"


class DeploymentType(Enum):
    ETLFLOW = "etl_flow"
    TARGETING = "targeting"
    RETRAINING = "retraining"
