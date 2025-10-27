"""
Script to sync the signals via the source
signals will be written to the session
Requirements:
- AWS Secret keys must be saved in the .env
"""

import pandas as pd
from datetime import datetime
from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.return_account_ids import return_account_ids
from general_functions.constants import return_api_url_prod
from general_functions.define_logging import define_logging


customer = "to teach"
start_date = "20250325"
end_date = "20250607"

logger = define_logging(
    f"sync-scripts/sync-signals/sync-signals-via-source-{customer}-{start_date}-{end_date}-{datetime.now()}"
)

url = return_api_url_prod()
logger.info(f"url = {url}")
account_id = return_account_ids()
account_id = [acc["id"] for acc in account_id if acc["name"] == customer]
if len(account_id) != 1:
    raise ValueError(f"Error in getting account: {account_id}")
account_id = account_id[0]
date_range = pd.date_range(start_date, end_date).strftime("%Y%m%d")
logger.info(f"date range: {date_range}")
for date in date_range:
    date = int(date)
    logger.info(f"date = {date}")
    payload = {"sources": ["googleAdwords"], "date": date}
    response = call_api_with_accountId(
        f"{url}/sources/sync", account_id, payload, logger
    )
    logger.info(f"response: {response}")
