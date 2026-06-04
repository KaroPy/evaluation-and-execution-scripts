import json
import logging
import os
import re
import sys

import pandas as pd
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.conncet_s3 import S3Connection
from general_functions.constants import return_api_url
from general_functions.return_workspace_ids import return_workspace_ids

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

BUCKET = "innkeepr-targeting-more"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "tier_size_config_overview.csv")

url = return_api_url()
s3 = S3Connection()

accounts = return_workspace_ids(tracking_started=False)
more_workspace = next((a for a in accounts if a["name"] == "More"), None)
if more_workspace is None:
    raise ValueError("'More' workspace not found")

workspace_name = more_workspace["name"]
workspace_id = more_workspace["id"]
logging.info(f"workspace: {workspace_name} ({workspace_id})")

audiences_raw = call_api_with_accountId(f"{url}/audiences/query", workspace_id, {}, logging)
audiences = pd.json_normalize(audiences_raw)
logging.info(f"found {len(audiences)} audiences")

DATE_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2})_tiersize_configs\.json$")

rows = []
for _, aud in audiences.iterrows():
    aud_id = aud["id"]
    aud_name = aud.get("name", aud_id)
    aud_ad_account = aud.get("properties.adAccountName", None)

    # main config dir + backup subdir
    prefixes = [
        f"more-aud-{aud_id}/tiersize_configs/",
        f"more-aud-{aud_id}/tiersize_configs/backup/",
    ]

    for prefix in prefixes:
        files = s3.list_files_with_pagination(BUCKET, prefix)
        json_files = [
            f for f in files if f.endswith(".json") and os.path.dirname(f) + "/" == prefix
        ]

        for file_key in json_files:
            filename = os.path.basename(file_key)
            m = DATE_PATTERN.match(filename)
            date_str = m.group(1) if m else filename

            try:
                raw = s3.read_json_from_aws(BUCKET, file_key)
                config = json.loads(raw)
            except Exception as exc:
                logging.warning(f"skipping {file_key}: {exc}")
                continue
            rows.append(
                {
                    "workspace": workspace_name,
                    "audience_id": aud_id,
                    "audience": aud_name,
                    "date_tier_size": date_str,
                    "other": json.dumps(config.get("other", None)),
                    "product_match": json.dumps(config.get("product_match", None)),
                    "aud_ad_account": aud_ad_account,
                }
            )

df = pd.DataFrame(
    rows,
    columns=[
        "workspace",
        "audience_id",
        "audience",
        "date_tier_size",
        "other",
        "product_match",
        "aud_ad_account",
    ],
)
df = df.sort_values(["audience", "audience_id", "date_tier_size"]).reset_index(drop=True)
df = df.drop_duplicates(subset=["workspace", "audience_id", "date_tier_size"])

df.to_csv(OUTPUT_PATH, index=False)
logging.info(f"wrote {len(df)} rows to {OUTPUT_PATH}")
print(df.to_string())
