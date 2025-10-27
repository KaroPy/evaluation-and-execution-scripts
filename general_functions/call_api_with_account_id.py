"""
Module to call api with account id
"""

import os
import json
import logging

import requests


def validate_response(jsonResponse, logger):
    """
    Function to validate response
    Parameters:
        jsonResponse (json): json response
        logger (logging): logger
    """
    if "messages" in jsonResponse and jsonResponse["messages"] is not None:
        if (
            jsonResponse["messages"][0]["type"] == "exception"
            or jsonResponse["messages"][0]["type"] == "error"
        ):
            raise Exception("Received error: " + json.dumps(jsonResponse))

    if "data" not in jsonResponse:
        raise Exception(f"Unexpected response body: {jsonResponse}")

    item_count = len(jsonResponse["data"])
    logger.info(f"Fetched {item_count} elements")


def make_http_post_call(
    api_url: str, payload: json, logger: logging, return_error=False
):
    """
    Function to make http post call
    Parameters:
        api_url (str): url of the api
        payload (json): payload of the api
        logger (logging): logger
    Returns:
        jsonBody (json): json body of the api
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.environ['SERVICE_TOKEN']}",
    }
    response = requests.post(api_url, headers=headers, data=payload)

    if response.status_code > 299:
        if not return_error:
            raise Exception(
                response.status_code, f"Unexpected server response: {response.text}"
            )

    jsonBody = json.loads(response.text)

    return jsonBody


def call_api_with_accountId(
    endpoint_url: str,
    accountID: str,
    content: json,
    logger: logging,
    ignore_response=False,
):
    """
    Function to call api with account id
    Parameters:
        api_url (str): url of the api
        endpoint (str): endpoint of the api
        accountID (str): account id of the customer
        content (json): content of the api
        logger (logging): logger
        ignore_response (bool): flag to ignore the response
    Returns:
        jsonBody (json): json body of the api
    """
    logger.info(f"Querying {endpoint_url}")

    payload = json.dumps({"content": content, "context": {"accountId": accountID}})

    jsonBody = make_http_post_call(
        endpoint_url, payload, logger, return_error=ignore_response
    )

    if not ignore_response:
        validate_response(jsonBody, logger)
        return jsonBody["data"]

    return jsonBody


def call_api_with_service_token(
    endpoint_url, service_token, content, logger, ignore_response=False
):
    logger.info(f"Querying {endpoint_url}")

    payload = json.dumps(
        {"content": content, "context": {"serviceToken": service_token}}
    )

    logger.info(f"Sending content {content}")

    jsonBody = make_http_post_call(endpoint_url, payload, logger)

    if not ignore_response:
        validate_response(jsonBody, logger)
        return jsonBody["data"]

    return


def send_to_innkeepr_api_paginated(api_url, accountID, content, logger, limit=None):
    logger.info(f"Querying {api_url}")
    next_page = 1
    if limit is None:
        pagination_content = {"page": next_page}
    else:
        pagination_content = {"limit": limit, "page": next_page}
    data = []
    while next_page is not None:
        payload = json.dumps(
            {
                "content": content,
                "pagination": pagination_content,
                "context": {"accountId": accountID},
            }
        )

        jsonBody = make_http_post_call(api_url, payload, logger)
        validate_response(jsonBody, logger)
        data += jsonBody["data"]
        next_page = jsonBody["pagination"]["next"]

    return data
