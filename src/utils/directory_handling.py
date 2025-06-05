import os


def create_directory_if_not_exists(directory_path):
    """
    Creates the directory if it does not exist.

    Args:
        directory_path (str): The path of the directory to create.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
