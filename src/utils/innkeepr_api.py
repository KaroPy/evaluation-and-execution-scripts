import json
import logging
import requests
from src.utils.errors import InnkeeprError
from src.utils.constants import return_service_token


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
    logging.info(f"Fetched {item_count} elements")


def make_http_post_call(
    api_url: str,
    payload: json,
    logger: logging,
    return_error=False,
    additional_headers=None,
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
    headers = {"Content-Type": "application/json"}
    if additional_headers is not None:
        headers = {**additional_headers, **headers}
    response = requests.post(api_url, headers=headers, data=payload)

    if response.status_code > 299:
        if not return_error:
            raise InnkeeprError(
                response.status_code, f"Unexpected server response: {response.text}"
            )

    jsonBody = json.loads(response.text)

    return jsonBody


def call_api_with_service_token(endpoint_url, content, logger, ignore_response=False):
    logging.info(f"Querying {endpoint_url}")
    service_token = return_service_token()

    payload = json.dumps({"content": content})

    logging.info(f"Sending content {content}")

    jsonBody = make_http_post_call(
        endpoint_url,
        payload,
        logger,
        additional_headers={"Authorization": f"Bearer {service_token}"},
    )

    if not ignore_response:
        validate_response(jsonBody, logger)
        return jsonBody["data"]

    return


def send_to_innkeepr_api_paginated(api_url, accountID, content, logger):
    logger.info(f"Querying {api_url}")
    service_token = return_service_token()
    data = []
    next_page = 1
    while next_page is not None:
        payload = json.dumps(
            {
                "content": content,
                "pagination": {"page": next_page},
                "context": {"accountId": accountID},
            }
        )

        jsonBody = make_http_post_call(
            api_url,
            payload,
            logger,
            additional_headers={"Authorization": f"Bearer {service_token}"},
        )
        validate_response(jsonBody, logger)
        data += jsonBody["data"]
        if "pagination" in jsonBody:
            next_page = jsonBody["pagination"]["next"]
        else:
            next_page = None

    return data
