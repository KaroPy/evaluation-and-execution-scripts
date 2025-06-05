import logging
from github import Github, Auth

from src.utils.constants import get_github_token
import yaml


def get_github_directories(repo, path, logger: logging, owner="Innkeepr"):
    """
    Connects to a GitHub repository and extracts existing directories from a certain path.

    Args:
        repo (str): Repository name.
        path (str): Path within the repository to list directories from.
        owner (str): GitHub username or organization.

    Returns:
        list: List of directory names at the specified path.
    """
    token = get_github_token()
    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(f"{owner}/{repo}")
    contents = repo.get_contents(path=path)
    directories = [item.name for item in contents if item.type == "dir"]
    return directories


def read_yaml_from_github(
    repo: str, dir_path: str, list_file_names: list, logger: logging, owner="Innkeepr"
):
    """
    Reads a YAML file from a GitHub repository.

    Args:
        repo (str): Repository name.
        dir_path (str): Path to directory in repository
        file_path (str): String of the YAML file in the repository to consider.
        logger (logging): Logger instance.
        owner (str): GitHub username or organization.
    Returns:
        dict: Parsed YAML content.
    """
    token = get_github_token()
    gh = Github(auth=Auth.Token(token))
    repo = gh.get_repo(f"{owner}/{repo}")
    logger.info(f"List files of {dir_path}")
    repo_dir_contents = repo.get_contents(dir_path)
    deployment_content = {}
    for content_file in repo_dir_contents:
        print(content_file, content_file.path, list_file_names)
        check = [
            file_name for file_name in list_file_names if file_name in content_file.path
        ]
        if len(check) == 1:
            file_name = check[0]
            key_name = content_file.path.split("/")[-1].split(".")[-2]
            yaml_content = yaml.safe_load(content_file.decoded_content.decode())
            logger.info(f"returned content: {yaml_content}")
            if yaml_content is None:
                yaml_content = []
            deployment_content[key_name] = yaml_content
            if len(yaml_content) == 0:
                logger.warning(f"No content found for {repo} and path {dir_path}")
        else:
            logger.warning(
                f"No content for {content_file.path} for considered {list_file_names}"
            )
            continue
    return deployment_content
