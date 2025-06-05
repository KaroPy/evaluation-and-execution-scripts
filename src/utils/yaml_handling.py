import yaml


def read_yaml_file(file_path):
    """
    Reads a YAML file and returns its contents as a Python object.

    Args:
        file_path (str): Path to the YAML file.

    Returns:
        dict or list: Parsed YAML content.
    """
    with open(file_path, "r") as file:
        return yaml.safe_load(file)
