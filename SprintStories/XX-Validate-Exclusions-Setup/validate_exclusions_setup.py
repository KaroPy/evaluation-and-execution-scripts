import os
from datetime import datetime

import pandas as pd
import yaml

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

    exclusions = df[(df["type"] == "exclusion") & df["name"].str.contains("Visitor", na=False)]
    if exclusions.empty:
        logger.info("  No exclusion audiences with 'Visitor' in name — skipping")
        continue

    logger.info(f"  Exclusion audiences with 'Visitor' in name: {len(exclusions)}")
    for _, row in exclusions.iterrows():
        rows.append(
            {
                "workspace_name": workspace_name,
                "id": row.get("id"),
                "name": row.get("name"),
                "type": row.get("type"),
                "targetingOutlookDays": row.get("config.targetingOutlookDays"),
                "source": row.get("source"),
                "created": row.get("created"),
            }
        )

result_df = pd.DataFrame(rows)
logger.info(f"Total exclusion audiences: {len(result_df)}")
result_df.to_csv(out, index=False)
logger.info(f"Saved → {out}")

# --- Step 2: Enrich with customer_specifications.yaml ---
with open(YAML_PATH) as f:
    customer_specs = yaml.safe_load(f)

enriched_rows = []
for _, row in result_df.iterrows():
    customer_key = (
        row["workspace_name"]
        .lower()
        .replace(" ", "")
        .replace("ö", "oe")
        .replace(".", "dot")
        .replace("-", "")
    )
    audience_id = row["id"]
    extra = {}
    if customer_key in customer_specs:
        customer = customer_specs[customer_key]
        if audience_id in customer:
            audience_spec = customer[audience_id]
            if isinstance(audience_spec, dict):
                extra = {k: str(v) for k, v in audience_spec.items()}
            logger.info(f"  Matched {audience_id} ({row['name']}) in customer '{customer_key}'")
    enriched_rows.append({**row.to_dict(), **extra})

enriched_df = pd.DataFrame(enriched_rows)

# --- Step 3: Validate setup per audience type ---
RULES = [
    {
        "pattern": "Innkeepr - 30-90d Visitors - Exclusion",
        "targetingOutlookDays": 90,
        "exclude_visitors": 30,
        "wrong_label": "wrong setup for 30-90d Visitors",
    },
    {
        "pattern": "Innkeepr - 30d Visitors - Exclusion",
        "targetingOutlookDays": 180,
        "exclude_visitors": 30,
        "wrong_label": "wrong setup for Innkeepr - 30d Visitors - Exclusion",
    },
    {
        "pattern": "Innkeepr - 90-180d Visitors - Exclusion",
        "targetingOutlookDays": 180,
        "exclude_visitors": 90,
        "wrong_label": "wrong setup for 90-180d Visitors",
    },
]


def _to_int(val):
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def get_setup_status(row):
    name = str(row.get("name", ""))
    for rule in RULES:
        if rule["pattern"] in name:
            print("rule", rule, "name: ", name)
            outlook_ok = _to_int(row.get("targetingOutlookDays")) == rule["targetingOutlookDays"]
            visitors_ok = _to_int(row.get("exclude_visitors")) == rule["exclude_visitors"]
            if outlook_ok and visitors_ok:
                return "correct setup"
            if name == "Innkeepr - 30d Visitors - Exclusion":
                ev = _to_int(row.get("exclude_visitors"))
                print(
                    "check for wrong label: ",
                    row.get("workspace_name"),
                    ev,
                    row.get("targetingOutlookDays"),
                )
                if _to_int(row.get("targetingOutlookDays")) == 90 and (ev == 30):
                    return "needs rename to Innkeepr - 30-90d Visitors - Exclusion"
                return rule["wrong_label"]
    return ""


enriched_df["setup_status"] = enriched_df.apply(get_setup_status, axis=1)

wrong = enriched_df[
    enriched_df["setup_status"].str.startswith("wrong")
    | enriched_df["setup_status"].str.startswith("needs rename")
]
logger.info(f"Wrong/needs-rename setups: {len(wrong)}")
for _, row in wrong.iterrows():
    logger.warning(
        f"  [{row['setup_status']}] {row['workspace_name']} — {row['name']} "
        f"(outlookDays={row.get('targetingOutlookDays')}, "
        f"exclude_visitors={row.get('exclude_visitors', 'n/a')})"
    )

enriched_df.to_csv(out, index=False)
logger.info(f"Enriched CSV saved → {out} ({len(enriched_df)} rows)")
