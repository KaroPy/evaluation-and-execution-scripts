import logging
import pandas as pd
import re
from datetime import datetime, timedelta
from general_functions.conncet_s3 import S3Connection
from general_functions.constants import return_api_url
from general_functions.call_api_with_account_id import call_api_with_accountId


def query_previous_models(account_id: str, audience: str, logger: logging):
    url = return_api_url()
    today = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=45)).strftime("%Y-%m-%d")
    models = call_api_with_accountId(
        f"{url}/models/query",
        account_id,
        {"created": {"$lt": today, "$gte": start_date}, "audienceId": audience},
        logging,
    )
    models = pd.json_normalize(models)
    models = models.sort_values(by="created")
    models = models.reset_index(drop=True)
    return models


def handle_model_rollback(tenant, audience, account_id, logger):
    """
    Handle model rollback when performance incidents are detected.

    Args:
        models: DataFrame containing model information
        tenant: Tenant name
        audience: Audience ID to update
        account_id: Account ID for API calls
        url: Base URL for API endpoints
        logger: Logger instance

    Returns:
        dict: Result of the rollback operation
    """
    models = query_previous_models(account_id, audience, logger)

    # Validate inputs
    if models.empty:
        raise ValueError("No models available for incident handling")

    # Get current model
    models = models.sort_values(by="created").reset_index(drop=True)
    current_model = models.loc[models["created"].idxmax()]

    logger.info(f"Current model: {current_model.id} created at {current_model.created}")

    # Check if rollback is needed
    rollback_needed = check_rollback_needed(current_model, tenant, logger)

    if not rollback_needed:
        logger.info("No rollback needed")
        return {"status": "no_action", "reason": "Current model is up to date"}

    # Perform rollback
    return perform_model_rollback(models, tenant, audience, account_id, logger)


def check_rollback_needed(current_model, tenant, logger):
    """Check if model rollback is required based on file dates."""
    s3 = S3Connection()
    tenant_sanitized = tenant.replace(" ", "").lower()
    bucket = f"test-innkeepr-targeting-{tenant_sanitized}"  # change production

    logger.info(f"Checking bucket: {bucket}")

    current_path_date = return_dates(current_model.path)
    files = s3.list_files_with_pagination(bucket, current_model.path)

    if not files:
        logger.warning(f"No files found in path: {current_model.path}")
        return False

    file_date = return_dates(files[0])
    rollback_needed = file_date < current_path_date

    logger.info(
        f"Rollback {'needed' if rollback_needed else 'not needed'}: "
        f"file_date={file_date}, current_path_date={current_path_date}"
    )

    return rollback_needed


def return_dates(text):
    print(text)
    match = re.findall(r"\d{4}-\d{2}-\d{2}", text)
    return match[-1]


def perform_model_rollback(models, tenant, audience, account_id, logger):
    """Perform the actual model rollback."""
    url = return_api_url()
    if len(models) < 2:
        raise ValueError("Not enough models to perform rollback")

    # Get previous model
    previous_model = models.iloc[-2]
    logger.info(f"Rolling back to model: {previous_model.id}")

    # Query previous model properties
    model_properties = get_model_properties(previous_model.id, account_id, url, logger)

    # Copy model files to new location
    new_paths = copy_prev_model_files(previous_model, tenant, logger)

    # Create new model with updated properties
    new_model_id = create_rollback_model(
        model_properties,
        new_paths,
        account_id,
        logger,
        manual_retrain_reason="Model Incident",
    )

    # Update audience with new model
    update_audience_with_prev_model(audience, new_model_id, account_id, logger)

    return {
        "status": "success",
        "previous_model_id": previous_model.id,
        "new_model_id": new_model_id,
        "new_paths": new_paths,
    }


def get_model_properties(model_id, account_id, url, logger):
    """Retrieve model properties from API."""
    response = call_api_with_accountId(
        f"{url}/models/query", account_id, {"id": model_id}, logger
    )

    if len(response) != 1:
        raise ValueError(f"Expected 1 model, got {len(response)}: {response}")

    return response[0]


def copy_prev_model_files(previous_model, tenant, logger):
    """Copy model files to new location with today's date."""
    s3 = S3Connection()
    tenant_sanitized = tenant.replace(" ", "").lower()
    bucket = f"test-innkeepr-targeting-{tenant_sanitized}"

    today = datetime.today().strftime("%Y-%m-%d")
    previous_path = previous_model.path
    previous_date = return_dates(previous_path)

    # Copy main model files
    new_model_path = previous_path.replace(previous_date, today)
    logger.info(f"Copying model files: {previous_path} -> {new_model_path}")
    s3.copy_all_files_recursively(bucket, previous_path, new_model_path)

    # Copy coding files
    coding_previous_path = f"{previous_path.split('_best_models')[0]}-coding"
    new_coding_path = coding_previous_path.replace(previous_date, today)
    logger.info(f"Copying coding files: {coding_previous_path} -> {new_coding_path}")
    s3.copy_all_files_recursively(bucket, coding_previous_path, new_coding_path)

    return {"model_path": new_model_path, "coding_path": new_coding_path}


def create_rollback_model(
    model_properties, new_paths, account_id, logger, manual_retrain_reason=None
):
    """Create a new model with rollback properties."""
    url = return_api_url()
    if manual_retrain_reason is None:
        raise ValueError("manual_retrain_reason is required")
    new_properties = model_properties.copy()
    new_properties.update(
        {
            "path": new_paths["model_path"],
            "created": datetime.now().isoformat() + "Z",
            "manualRetrainReason": manual_retrain_reason,
        }
    )
    new_properties.pop("id", None)

    logger.info(f"Creating new model with properties: {new_properties}")

    # TODO: Implement actual API call to create model
    # response = call_api_with_accountId(
    #     f"{url}/models/store",
    #     account_id,
    #     new_properties,
    #     logger
    # )
    # return response["id"]

    # TODO: Temporary placeholder
    return "new_model_id_placeholder"


def update_audience_with_prev_model(audience_id, model_id, account_id, logger):
    """Update audience with new model."""
    url = return_api_url()
    payload = {"id": audience_id, "config": {"model": model_id}}

    logger.info(f"Updating audience {audience_id} with model {model_id}")

    # TODO: Implement actual API call to update audience
    # call_api_with_accountId(
    #     f"{url}/audiences/update",
    #     account_id,
    #     payload,
    #     logger
    # )
