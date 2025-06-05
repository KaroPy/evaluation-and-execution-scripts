import logging
import warnings
import pandas as pd


def get_logger(logging_name):
    # handle logging
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    logging.getLogger("awswrangler").setLevel(logging.CRITICAL)
    # rm interactive logging for lstm
    logging.getLogger("absl").setLevel(logging.ERROR)
    # Suppress all warnings from Pandas
    warnings.simplefilter(action="ignore", category=UserWarning)
    pd.options.mode.chained_assignment = None
    # initialize logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    ch = logging.StreamHandler()
    fh = logging.FileHandler(logging_name + ".log", "w")
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add the handlers to the logger
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger
