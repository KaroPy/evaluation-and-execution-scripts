import os
import yaml
import pandas as pd
from datetime import datetime

from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.constants import return_api_url
from general_functions.define_logging import define_logging
from general_functions.return_workspace_ids import return_workspace_ids

PATH = "SprintStories/XX-Validate-Exclusions-Setup/"
YAML_PATH = "../innkeepr-analytics/configs/customer_specifications.yaml"
os.makedirs(f"{PATH}logs", exist_ok=True)

logger = define_logging(
    f"{PATH}logs/validate_exclusions_setup-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

url = return_api_url()
workspaces = return_workspace_ids()
out = f"{PATH}exclusion_audiences_{datetime.now().strftime('%Y%m%d')}.csv"

# --- Step 1: Extract exclusion audiences across all workspaces ---
rows = []
for workspace in workspaces:
    account_id = workspace["id"]
    workspace_name = workspace["name"]
    logger.info(f"=== Workspace: {workspace_name} ({account_id}) ===")

    audiences = call_api_with_accountId(f"{url}/audiences/query", account_id, {}, logger)
    if not audiences:
        logger.info("  No audiences found — skipping")
        continue

    df = pd.json_normalize(audiences)
    if "type" not in df.columns:
        logger.info("  No type column in response — skipping")
        continue

    exclusions = df[df["type"] == "exclusion"]
    if exclusions.empty:
        logger.info("  No exclusion audiences — skipping")
        continue

    logger.info(f"  Exclusion audiences found: {len(exclusions)}")
    for _, row in exclusions.iterrows():
        rows.append(
            {
                "workspace_name": workspace_name,
                "id": row.get("id"),
                "name": row.get("name"),
                "type": row.get("type"),
                "targetingOutlookDays": row.get("config.targetingOutlookDays"),
            }
        )

result_df = pd.DataFrame(rows)
logger.info(f"Total exclusion audiences: {len(result_df)}")
result_df.to_csv(out, index=False)
logger.info(f"Saved → {out}")

# --- Step 2: Enrich with customer_specifications.yaml ---
with open(YAML_PATH, "r") as f:
    customer_specs = yaml.safe_load(f)

enriched_rows = []
for _, row in result_df.iterrows():
    customer_key = row["workspace_name"].lower()
    audience_id = row["id"]
    extra = {}
    if customer_key in customer_specs:
        customer = customer_specs[customer_key]
        if audience_id in customer:
            audience_spec = customer[audience_id]
            if isinstance(audience_spec, dict):
                extra = {k: str(v) for k, v in audience_spec.items()}
            logger.info(
                f"  Matched {audience_id} ({row['name']}) in customer '{customer_key}'"
            )
    enriched_rows.append({**row.to_dict(), **extra})

enriched_df = pd.DataFrame(enriched_rows)
enriched_df.to_csv(out, index=False)
logger.info(f"Enriched CSV saved → {out} ({len(enriched_df)} rows)")
