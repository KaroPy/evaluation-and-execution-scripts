import json
import requests
from general_functions.constants import (
    return_databricks_api_token,
    return_databricks_url,
)


def query_databricks(endpoint, header, params=None):
    response = requests.get(endpoint, headers=header, params=params)

    # Check if response was successful
    if response.status_code != 200:
        raise Exception(f"Failed to query {endpoint}: {response.text}")
    response_json = response.json()
    return response_json


def query_databricks_paginated(endpoint, header, key=None, params=None):
    page_token = None
    jobs = []
    if key is None:
        raise ValueError("key is required")

    # Loop until all pages have been retrieved
    while True:
        # Set page token in query parameters if available
        if params is None:
            params = {}
        if page_token:
            params["page_token"] = page_token

        response_json = query_databricks(endpoint, header, params=params)

        # Extract jobs from response
        jobs.extend(response_json[key])

        # Check if there are more pages
        if "page_token" in response_json:
            page_token = response_json["page_token"]
        elif "next_page_token" in response_json:
            page_token = response_json["next_page_token"]
        else:
            break
        if page_token is None or len(page_token) == 0:
            break

    return jobs


def post_databricks(endpoint, headers, payload):
    response = requests.post(endpoint, headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to query {endpoint}: {response.text}")
    return response


class DatabricksClient:
    def __init__(self):
        self.databricks_url = return_databricks_url()
        self.api_token = return_databricks_api_token()
        self.headers = {"Authorization": f"Bearer {self.api_token}"}

    def return_jobs(self):
        endpoint = f"{self.databricks_url}api/2.0/jobs/list"
        clusters = query_databricks_paginated(endpoint, self.headers, key="jobs")

        return clusters

    def update_job(self, job_config: json):
        endpoint = f"{self.databricks_url}api/2.2/jobs/update"
        post_databricks(endpoint, self.headers, job_config)

    def return_clusters(self):
        endpoint = f"{self.databricks_url}api/2.1/clusters/list"
        clusters = query_databricks_paginated(endpoint, self.headers, key="clusters")

        return clusters

    def return_custer_info(self, cluster_id):
        endpoint = f"{self.databricks_url}api/2.1/clusters/get"
        params = {"cluster_id": cluster_id}
        cluster_info = query_databricks(endpoint, self.headers, params=params)
        return cluster_info

    def update_cluster(self, cluster_config: json):
        endpoint = f"{self.databricks_url}api/2.1/clusters/edit"
        post_databricks(endpoint, self.headers, cluster_config)
