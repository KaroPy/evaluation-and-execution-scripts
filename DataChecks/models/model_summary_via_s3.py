import json
import logging
import os
import re
import sys
import tempfile

import h5py
import pandas as pd
import tensorflow as tf
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from general_functions.call_api_with_account_id import call_api_with_accountId
from general_functions.conncet_s3 import S3Connection
from general_functions.constants import return_api_url
from general_functions.return_workspace_ids import return_workspace_ids
from general_functions.sanitize_accout_name import sanitize_account_name

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

TARGET_DATE = "2026-01-06"
TARGET_DATE_NEXT = TARGET_DATE[:8] + str(int(TARGET_DATE[8:]) + 1).zfill(2)
TARGET_ACCOUNT = ["More"]

s3 = S3Connection()
accounts = return_workspace_ids()
accounts = [a for a in accounts if a["name"] in TARGET_ACCOUNT]
url = return_api_url()
logging.info(f"url={url}, accounts={len(accounts)}")


def read_keras_metadata(h5_path: str) -> dict:
    meta = {}
    with h5py.File(h5_path, "r") as f:
        for k, v in f.attrs.items():
            try:
                meta[k] = json.loads(v) if isinstance(v, str) and v.startswith("{") else v
            except Exception:
                meta[k] = v
    return meta


def get_model_files(bucket: str, prefix: str) -> list:
    all_files = s3.list_files_with_pagination(bucket, prefix)
    return [f for f in all_files if f.endswith(".h5") and not f.lower().endswith(".weights.h5")]


# --- collect models from API ---
all_models = []
for account in accounts:
    models = call_api_with_accountId(
        f"{url}/models/query",
        account["id"],
        {"created": {"$gte": TARGET_DATE, "$lt": TARGET_DATE_NEXT}},
        logging,
    )
    if not models:
        continue
    df = pd.json_normalize(models)
    df["account"] = account["name"]
    df["account_sanitized"] = sanitize_account_name(account["name"])
    all_models.append(df)

if not all_models:
    print(f"No models found for {TARGET_DATE}")
    sys.exit(0)

all_models_df = pd.concat(all_models, ignore_index=True)
logging.info(
    f"found {len(all_models_df)} models across {all_models_df['account'].nunique()} accounts"
)

# --- inspect each model ---
summaries = []

for _, row in all_models_df.iterrows():
    bucket = f"innkeepr-targeting-{row['account_sanitized']}"
    model_prefix = row["path"]

    try:
        h5_keys = get_model_files(bucket, model_prefix)
    except Exception as exc:
        logging.warning(f"listing {bucket}/{model_prefix}: {exc}")
        continue

    if not h5_keys:
        logging.info(f"no .h5 files under {bucket}/{model_prefix}")
        continue

    for h5_key in h5_keys:
        filename = os.path.basename(h5_key)
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
        file_date = date_match.group(0) if date_match else None

        print(f"\n{'=' * 70}")
        print(f"Account  : {row['account']}")
        print(f"Audience : {row.get('audience', 'n/a')}")
        print(f"File     : {h5_key}")
        print(f"File date: {file_date}")

        with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            s3.s3.download_file(bucket, h5_key, tmp_path)

            meta = read_keras_metadata(tmp_path)
            keras_version = meta.get("keras_version", "n/a")
            backend = meta.get("backend", "n/a")
            created_ts = meta.get(
                "date_of_training",
                meta.get("created", meta.get("training_start", "not stored in h5")),
            )
            print(f"Keras ver: {keras_version}  |  backend: {backend}")
            print(f"Created  : {created_ts}")
            print(
                "H5 attrs :",
                {k: v for k, v in meta.items() if k not in ("model_config", "training_config")},
            )

            try:
                model = tf.keras.models.load_model(tmp_path, compile=False)
                print("\nModel summary:")
                model.summary()
                params = model.count_params()
            except ValueError as load_err:
                print(f"  [WARN] load_model failed ({load_err}) — skipping summary, metadata only")
                params = None

            summaries.append(
                {
                    "account": row["account"],
                    "audience": row.get("audience"),
                    "file": h5_key,
                    "file_date": file_date,
                    "keras_version": keras_version,
                    "created_ts": created_ts,
                    "params": params,
                }
            )

        except Exception as exc:
            logging.exception(f"{h5_key}: {exc}")
        finally:
            os.unlink(tmp_path)

print(f"\n{'=' * 70}")
print("Summary table:")
summary_df = pd.DataFrame(summaries)
print(summary_df.to_string())
