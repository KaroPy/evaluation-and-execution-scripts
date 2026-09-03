"""
Iterate all workspaces, query models created since a given date, and collect
rows where manualRetrainReason is set (not null / not empty).

Usage (from repo root):
  PYTHONPATH=. .venv/bin/python3 Analysis/DTW_Incidents/query_manual_retrain_reasons.py
  PYTHONPATH=. .venv/bin/python3 Analysis/DTW_Incidents/query_manual_retrain_reasons.py --since 2026-05-01
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from general_functions.call_api_with_account_id import (  # noqa: E402
    call_api_with_accountId,
    send_to_innkeepr_api_paginated,
)
from general_functions.constants import return_api_url  # noqa: E402
from general_functions.return_workspace_ids import return_workspace_ids  # noqa: E402

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SINCE = "2026-05-01"
DEFAULT_OUT = (
    Path(__file__).resolve().parent / "manual_retrain_reasons_since_2026-05-01.csv"
)

KEEP_COLS = [
    "workspace_name",
    "workspace_id",
    "id",
    "created",
    "type",
    "audience",
    "path",
    "manualRetrainReason",
    "goal",
    "f1Score",
]


def _models_url(api_url: str) -> str:
    base = api_url.rstrip("/")
    if base.endswith("/api"):
        return f"{base}/models/query"
    return f"{base}/api/models/query"


def _has_manual_retrain_reason(value) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def query_models_since(workspace_id: str, since: str, use_pagination: bool) -> list[dict]:
    content = {"created": {"$gte": since}}
    url = _models_url(return_api_url())
    if use_pagination:
        return send_to_innkeepr_api_paginated(url, workspace_id, content, logger)
    return call_api_with_accountId(url, workspace_id, content, logger) or []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find models with manualRetrainReason across all workspaces."
    )
    parser.add_argument("--since", default=DEFAULT_SINCE, help="ISO date lower bound for model.created")
    parser.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help="CSV output path for matching models",
    )
    parser.add_argument(
        "--paginate",
        action="store_true",
        help="Use paginated models/query (safer for large workspaces)",
    )
    parser.add_argument(
        "--workspace",
        action="append",
        default=None,
        help="Optional workspace name filter (repeatable)",
    )
    args = parser.parse_args()

    workspaces = return_workspace_ids(tracking_started=False)
    if args.workspace:
        wanted = {w.lower() for w in args.workspace}
        workspaces = [w for w in workspaces if w["name"].lower() in wanted]

    logger.info("Workspaces to scan: %s (since %s)", len(workspaces), args.since)

    rows: list[dict] = []
    totals = {"workspaces": 0, "models": 0, "with_reason": 0, "errors": 0}

    for ws in workspaces:
        totals["workspaces"] += 1
        name, wid = ws["name"], ws["id"]
        try:
            models = query_models_since(wid, args.since, use_pagination=args.paginate)
        except Exception as exc:
            totals["errors"] += 1
            logger.exception("Failed for workspace %s (%s): %s", name, wid, exc)
            continue

        totals["models"] += len(models)
        matched = [m for m in models if _has_manual_retrain_reason(m.get("manualRetrainReason"))]
        totals["with_reason"] += len(matched)
        logger.info(
            "%s: %s models since %s, %s with manualRetrainReason",
            name,
            len(models),
            args.since,
            len(matched),
        )

        for model in matched:
            rows.append(
                {
                    "workspace_name": name,
                    "workspace_id": wid,
                    "id": model.get("id"),
                    "created": model.get("created"),
                    "type": model.get("type"),
                    "audience": model.get("audience"),
                    "path": model.get("path"),
                    "manualRetrainReason": model.get("manualRetrainReason"),
                    "goal": model.get("goal"),
                    "f1Score": model.get("f1Score"),
                }
            )

    df = pd.DataFrame(rows, columns=KEEP_COLS)
    if not df.empty and "created" in df.columns:
        df = df.sort_values(["created", "workspace_name"], ascending=[False, True]).reset_index(
            drop=True
        )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print("\n=== Summary ===")
    print(f"workspaces scanned : {totals['workspaces']}")
    print(f"models since {args.since}: {totals['models']}")
    print(f"with manualRetrainReason: {totals['with_reason']}")
    print(f"workspace errors   : {totals['errors']}")
    print(f"wrote              : {out_path}")

    if not df.empty:
        print("\nReasons:")
        print(df["manualRetrainReason"].value_counts(dropna=False).to_string())
        print("\nSample:")
        print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
