import os
import re
import logging
from datetime import datetime
from typing import List, Tuple


def search_repo(
    root_dir: str,
    search_terms: List[str],
    use_regex: bool = False,
    file_extensions: List[str] = None,
    exclude_dirs: List[str] = None,
) -> List[Tuple[str, int, str]]:
    """
    Search for given strings or regex patterns in all code files of a repository.

    Args:
        root_dir (str): Root directory of the repository.
        search_terms (List[str]): List of strings or regex patterns to search for.
        use_regex (bool): If True, treat search_terms as regex patterns. Default is False.
        file_extensions (List[str]): Optional list of file extensions to include (e.g., ['.py', '.js']).
        exclude_dirs (List[str]): Optional list of directory names to skip (e.g., ['.git', 'venv']).

    Returns:
        List[Tuple[str, int, str]]: A list of (file_path, line_number, line_text) for matches.
    """
    matches = []
    exclude_dirs = exclude_dirs or [
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        "build",
    ]

    for root, dirs, files in os.walk(root_dir):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in exclude_dirs]

        for filename in files:
            # Filter by file extension if provided
            if file_extensions and not any(
                filename.endswith(ext) for ext in file_extensions
            ):
                continue

            file_path = os.path.join(root, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line_number, line in enumerate(f, start=1):
                        for term in search_terms:
                            if (use_regex and re.search(term, line)) or (
                                not use_regex and term in line
                            ):
                                matches.append((file_path, line_number, line.strip()))
                                break
            except (UnicodeDecodeError, PermissionError):
                # Skip files that can't be read
                continue

    return matches


if __name__ == "__main__":
    # Example usage:
    git_dir = "innkeepr-databricks"
    logging.basicConfig(
        level=logging.INFO,
        filename=f"search_repo_{git_dir}_{datetime.now()}.log",
        filemode="w",
        # format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        format="%(message)s",
    )
    repo_path = f"/Users/karolinegriesbach/Documents/Innkeepr/Git/{git_dir}/"  # current directory (VS Code workspace)
    logging.info("Starting search for occurrences in directory %s", repo_path)
    search_terms = [
        ".performance_incidents",
        ".attributed_conversions",
        ".datashifts",
        ".analytics_exclude_dates",
        ".prefect_logs",
        ".datashift_events_snapshot",
        ".audience_bot_data",
        ".datashift_treatments_snapshot",
    ]
    search_terms = [item.replace(".", "") for item in search_terms]
    results = search_repo(
        repo_path,
        search_terms,
        use_regex=False,
        file_extensions=[".py", ".js", ".ts", ".html"],
    )

    for file_path, line_num, text in results:
        logging.info(
            f"{file_path.removeprefix('/Users/karolinegriesbach/Documents/Innkeepr/Git/')}:{line_num}: {text}"
        )
