"""
Final merge sanity check for goals-to-objectives.

For each customer and each distinct signal.objective, compare targeting.history
for at least one active signal with that objective: a recent run (last ~24h,
lookback up to 3 days) vs one older than 7 days. Compare probability
distributions and treatment.value_counts().

Warn when any mismatch exceeds the threshold. Results are written to CSV and JSON.

Usage (from repo root):
    python SprintStories/EN-xx-goals-to-objectives/final_merge_check.py
    python SprintStories/EN-xx-goals-to-objectives/final_merge_check.py --customer Rosental
    python SprintStories/EN-xx-goals-to-objectives/final_merge_check.py --seed 42
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import awswrangler as wr
import pandas as pd
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SCRIPT_DIR))

from goals_to_objectives import query_all  # ruff: ignore[module-import-not-at-top-of-file]

from general_functions.conncet_s3 import (
    S3Connection,
)
from general_functions.constants import (
    return_api_url,
)
from general_functions.return_workspace_ids import (
    return_workspace_ids,
)

DATA_DIR = SCRIPT_DIR / "data"
LOGS_DIR = SCRIPT_DIR / "logs"

MISMATCH_THRESHOLD = 0.05
RECENT_MAX_AGE_DAYS = 1
# Some workspaces only write targeting.history every ~3 days; allow a short lookback.
RECENT_LOOKBACK_DAYS = 3
OLD_MIN_AGE_DAYS = 15
PROB_COLUMNS = ("conv_prob", "counterfactual", "probability", "action_prob")


def default_output_paths() -> tuple[Path, Path]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"final_merge_check_{stamp}"
    return DATA_DIR / f"{stem}.csv", DATA_DIR / f"{stem}.json"


def json_path_from_csv(csv_path: Path) -> Path:
    return csv_path.with_suffix(".json")


def default_log_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return LOGS_DIR / f"final_merge_check_{stamp}.log"


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root_logger.addHandler(file_handler)

    for noisy in ("boto3", "botocore", "s3transfer", "urllib3", "awswrangler"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)
    warnings.simplefilter(action="ignore", category=UserWarning)


def normalize_customer_filter(customers: list[str] | None) -> set[str] | None:
    if not customers:
        return None
    names: set[str] = set()
    for item in customers:
        for part in item.split(","):
            name = part.strip()
            if name:
                names.add(name)
    return names or None


def parse_history_date(date_str: str) -> datetime | None:
    try:
        return datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def get_targeting_dates(s3: S3Connection, account_id: str) -> list[str]:
    prefixes = s3.list_files_with_pagination(
        bucket_name=account_id,
        prefix="targeting.history/",
        delimiter="/",
    )
    dates: list[str] = []
    for prefix in prefixes:
        if "meta" in prefix or prefix == "targeting.history/":
            continue
        date_str = prefix.rstrip("/").split("/")[-1]
        if parse_history_date(date_str) is not None:
            dates.append(date_str)
    return sorted(dates, reverse=True)


def targeting_history_path(account_id: str, date: str, signal_id: str) -> str:
    return f"s3://{account_id}/targeting.history/{date}/{signal_id}.parquet"


def load_targeting_history(
    account_id: str,
    date: str,
    signal_id: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    path = targeting_history_path(account_id, date, signal_id)
    logger.info("    Reading %s", path)
    try:
        return wr.s3.read_parquet(path)
    except Exception as exc:
        logger.debug("    Could not read %s: %s", path, exc)
        return pd.DataFrame()


def find_history_date(
    s3: S3Connection,
    account_id: str,
    signal_id: str,
    dates: list[str],
    *,
    min_age_days: float | None = None,
    max_age_days: float | None = None,
    logger: logging.Logger,
) -> tuple[str | None, pd.DataFrame]:
    """
    Iterate S3 date folders and return the first folder that has this signal's
    parquet and whose age falls within [min_age_days, max_age_days].
    """
    now = datetime.now(timezone.utc)
    for date_str in dates:
        date_dt = parse_history_date(date_str)
        if date_dt is None:
            continue
        age_days = (now - date_dt).total_seconds() / 86400.0
        if min_age_days is not None and age_days < min_age_days:
            continue
        if max_age_days is not None and age_days > max_age_days:
            continue

        # Prefer an existence check before a full parquet read when possible.
        keys = s3.list_files_with_pagination(
            bucket_name=account_id,
            prefix=f"targeting.history/{date_str}/{signal_id}.parquet",
        )
        if not any(key.endswith(f"{signal_id}.parquet") for key in keys):
            continue

        df = load_targeting_history(account_id, date_str, signal_id, logger)
        if not df.empty:
            return date_str, df
    return None, pd.DataFrame()


def resolve_probability_series(df: pd.DataFrame) -> pd.Series | None:
    for col in PROB_COLUMNS:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    return None


def treatment_proportions(df: pd.DataFrame) -> dict[str, float]:
    if df.empty or "treatment" not in df.columns:
        return {}
    counts = df["treatment"].astype(str).value_counts(normalize=True, dropna=False)
    return {str(k): float(v) for k, v in counts.items()}


def max_proportion_mismatch(
    recent: dict[str, float],
    old: dict[str, float],
) -> float:
    keys = set(recent) | set(old)
    if not keys:
        return 0.0
    return max(abs(recent.get(key, 0.0) - old.get(key, 0.0)) for key in keys)


def probability_stats(series: pd.Series | None) -> dict[str, float | None]:
    if series is None:
        return {"mean": None, "std": None, "count": 0}
    clean = series.dropna()
    if clean.empty:
        return {"mean": None, "std": None, "count": 0}
    return {
        "mean": float(clean.mean()),
        "std": float(clean.std(ddof=0)),
        "count": len(clean),
    }


def relative_or_absolute_mismatch(
    recent_mean: float | None,
    old_mean: float | None,
) -> float | None:
    if recent_mean is None or old_mean is None:
        return None
    denom = max(abs(old_mean), abs(recent_mean), 1e-12)
    return abs(recent_mean - old_mean) / denom


def signal_objective_id(signal: dict) -> str | None:
    objective = signal.get("objective")
    if objective is None or objective == "":
        return None
    if isinstance(objective, dict):
        objective_id = objective.get("id")
        return str(objective_id) if objective_id else None
    return str(objective)


def collect_active_signals_with_objective(
    api_url: str,
    workspaces: list[dict],
    logger: logging.Logger,
) -> dict[str, list[dict]]:
    """Return active signals that have an objective, grouped by workspace id."""
    by_workspace: dict[str, list[dict]] = {}
    for workspace in workspaces:
        account_id = workspace["id"]
        account_name = workspace["name"]
        logger.info("=== Collecting signals: %s (%s) ===", account_name, account_id)
        signals = query_all(
            f"{api_url}/api/signals/query",
            account_id,
            {"status": "active"},
            logger,
        )
        with_objective: list[dict] = []
        for signal in signals:
            signal_id = signal.get("id")
            objective_id = signal_objective_id(signal)
            if not signal_id or not objective_id:
                continue
            with_objective.append(
                {
                    "workspace.id": account_id,
                    "workspace.name": account_name,
                    "signal.id": signal_id,
                    "signal.name": signal.get("name"),
                    "signal.type": signal.get("type"),
                    "signal.objective": objective_id,
                }
            )
        by_workspace[account_id] = with_objective
        n_objectives = len({row["signal.objective"] for row in with_objective})
        logger.info(
            "  Active signals: %s | with objective: %s | distinct objectives: %s",
            len(signals),
            len(with_objective),
            n_objectives,
        )
    return by_workspace


def compare_signal_histories(
    s3: S3Connection,
    candidate: dict,
    dates: list[str],
    logger: logging.Logger,
) -> dict | None:
    account_id = candidate["workspace.id"]
    signal_id = candidate["signal.id"]

    new_date, new_df = find_history_date(
        s3,
        account_id,
        signal_id,
        dates,
        min_age_days=None,
        max_age_days=RECENT_LOOKBACK_DAYS,
        logger=logger,
    )
    if new_date is None:
        logger.info(
            "  Skip %s / %s: no recent targeting.history within %s days",
            candidate["workspace.name"],
            signal_id,
            RECENT_LOOKBACK_DAYS,
        )
        return None

    new_age = (
        datetime.now(timezone.utc) - parse_history_date(new_date)  # type: ignore[operator]
    ).total_seconds() / 86400.0
    if new_age > RECENT_MAX_AGE_DAYS:
        logger.warning(
            "  Recent history for %s / %s is %.1f days old (lookback allowed up to %s days)",
            candidate["workspace.name"],
            signal_id,
            new_age,
            RECENT_LOOKBACK_DAYS,
        )

    old_date, old_df = find_history_date(
        s3,
        account_id,
        signal_id,
        dates,
        min_age_days=OLD_MIN_AGE_DAYS,
        max_age_days=None,
        logger=logger,
    )
    if old_date is None:
        logger.info(
            "  Skip %s / %s: no targeting.history older than %s days",
            candidate["workspace.name"],
            signal_id,
            OLD_MIN_AGE_DAYS,
        )
        return None

    new_rows = len(new_df)
    old_rows = len(old_df)
    perc_rows = (new_rows / old_rows) if old_rows else None

    new_props = treatment_proportions(new_df)
    old_props = treatment_proportions(old_df)
    treatment_mismatch = max_proportion_mismatch(new_props, old_props)

    new_prob = probability_stats(resolve_probability_series(new_df))
    old_prob = probability_stats(resolve_probability_series(old_df))
    prob_mismatch = relative_or_absolute_mismatch(
        new_prob["mean"],  # type: ignore[arg-type]
        old_prob["mean"],  # type: ignore[arg-type]
    )

    treatment_warning = treatment_mismatch > MISMATCH_THRESHOLD
    probability_warning = prob_mismatch is not None and prob_mismatch > MISMATCH_THRESHOLD
    any_warning = treatment_warning or probability_warning

    if any_warning:
        logger.warning(
            "  MISMATCH > %.0f%% for %s / %s (%s): treatment=%.4f prob=%s perc_rows=%s",
            MISMATCH_THRESHOLD * 100,
            candidate["workspace.name"],
            signal_id,
            candidate.get("signal.name"),
            treatment_mismatch,
            f"{prob_mismatch:.4f}" if prob_mismatch is not None else "n/a",
            f"{perc_rows:.4f}" if perc_rows is not None else "n/a",
        )
    else:
        logger.info(
            "  OK %s / %s: treatment_mismatch=%.4f prob_mismatch=%s perc_rows=%s",
            candidate["workspace.name"],
            signal_id,
            treatment_mismatch,
            f"{prob_mismatch:.4f}" if prob_mismatch is not None else "n/a",
            f"{perc_rows:.4f}" if perc_rows is not None else "n/a",
        )

    return {
        "workspace.id": account_id,
        "workspace.name": candidate["workspace.name"],
        "signal.id": signal_id,
        "signal.name": candidate.get("signal.name"),
        "signal.type": candidate.get("signal.type"),
        "signal.objective": candidate.get("signal.objective"),
        "new.date": new_date,
        "new.age_days": round(new_age, 3),
        "new.rows": new_rows,
        "old.date": old_date,
        "old.age_days": round(
            (
                datetime.now(timezone.utc) - parse_history_date(old_date)  # type: ignore[operator]
            ).total_seconds()
            / 86400.0,
            3,
        ),
        "old.rows": old_rows,
        "perc_rows": round(perc_rows, 6) if perc_rows is not None else None,
        "new.treatment_proportions": json.dumps(new_props, sort_keys=True),
        "old.treatment_proportions": json.dumps(old_props, sort_keys=True),
        "treatment.mismatch": round(treatment_mismatch, 6),
        "treatment.warning": treatment_warning,
        "new.prob_mean": new_prob["mean"],
        "new.prob_std": new_prob["std"],
        "new.prob_count": new_prob["count"],
        "old.prob_mean": old_prob["mean"],
        "old.prob_std": old_prob["std"],
        "old.prob_count": old_prob["count"],
        "probability.mismatch": (round(prob_mismatch, 6) if prob_mismatch is not None else None),
        "probability.warning": probability_warning,
        "any.warning": any_warning,
        "mismatch.threshold": MISMATCH_THRESHOLD,
    }


def group_candidates_by_objective(
    candidates: list[dict],
) -> dict[str, list[dict]]:
    by_objective: dict[str, list[dict]] = {}
    for row in candidates:
        objective_id = row["signal.objective"]
        by_objective.setdefault(objective_id, []).append(row)
    return by_objective


def compare_one_signal_per_objective(
    s3: S3Connection,
    candidates_by_workspace: dict[str, list[dict]],
    rng: random.Random,
    logger: logging.Logger,
) -> list[dict]:
    """
    For each customer and each distinct objective, try active signals with that
    objective until one yields a comparable new/old targeting.history pair.
    """
    dates_by_workspace: dict[str, list[str]] = {}
    results: list[dict] = []

    workspace_ids = list(candidates_by_workspace.keys())
    rng.shuffle(workspace_ids)

    for workspace_id in workspace_ids:
        candidates = list(candidates_by_workspace[workspace_id])
        if not candidates:
            continue

        workspace_name = candidates[0]["workspace.name"]
        by_objective = group_candidates_by_objective(candidates)
        objective_ids = list(by_objective.keys())
        rng.shuffle(objective_ids)

        if workspace_id not in dates_by_workspace:
            dates_by_workspace[workspace_id] = get_targeting_dates(s3, workspace_id)
            logger.info(
                "  Workspace %s: %s targeting.history dates | %s candidate signals | %s objectives",
                workspace_name,
                len(dates_by_workspace[workspace_id]),
                len(candidates),
                len(objective_ids),
            )

        for objective_id in objective_ids:
            objective_candidates = list(by_objective[objective_id])
            rng.shuffle(objective_candidates)
            logger.info(
                "  Objective %s: trying %s signal(s)",
                objective_id,
                len(objective_candidates),
            )

            row: dict | None = None
            for candidate in objective_candidates:
                row = compare_signal_histories(
                    s3,
                    candidate,
                    dates_by_workspace[workspace_id],
                    logger,
                )
                if row is not None:
                    break

            if row is None:
                logger.warning(
                    "  No usable signal for customer %s objective %s",
                    workspace_name,
                    objective_id,
                )
                continue

            results.append(row)

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare recent vs older targeting.history for at least one active "
            "signal per distinct objective on each customer."
        )
    )
    parser.add_argument(
        "--customer",
        action="append",
        dest="customers",
        metavar="NAME",
        help="Limit to one or more workspace names, e.g. --customer Rosental",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible signal selection",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="CSV output path (default: data/final_merge_check_<timestamp>.csv)",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="JSON output path (default: same stem as CSV with .json)",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(REPO_ROOT / ".env")

    log_path = args.log_file or default_log_path()
    setup_logging(log_path)
    logger = logging.getLogger(__name__)
    logger.info("Logging to %s", log_path)

    rng = random.Random(args.seed)
    api_url = return_api_url().rstrip("/")
    customer_filter = normalize_customer_filter(args.customers)
    workspaces = return_workspace_ids(tracking_started=False)

    if customer_filter:
        workspaces = [ws for ws in workspaces if ws["name"] in customer_filter]
        missing = customer_filter - {ws["name"] for ws in workspaces}
        if missing:
            logger.warning("Workspace(s) not found: %s", ", ".join(sorted(missing)))

    if not workspaces:
        logger.info("No workspaces to process.")
        return

    s3 = S3Connection()
    candidates_by_workspace = collect_active_signals_with_objective(api_url, workspaces, logger)
    n_candidates = sum(len(rows) for rows in candidates_by_workspace.values())
    n_objectives = sum(
        len({row["signal.objective"] for row in rows}) for rows in candidates_by_workspace.values()
    )
    workspaces_with_candidates = sum(1 for rows in candidates_by_workspace.values() if rows)
    if n_candidates == 0:
        logger.info("No active signals with an objective found.")
        return

    logger.info(
        "Comparing one signal per objective: %s candidates / %s objectives across %s/%s workspaces",
        n_candidates,
        n_objectives,
        workspaces_with_candidates,
        len(workspaces),
    )

    results = compare_one_signal_per_objective(s3, candidates_by_workspace, rng, logger)

    df = pd.DataFrame(results)
    default_csv, default_json = default_output_paths()
    csv_path = args.output or default_csv
    json_path = args.json_output or (json_path_from_csv(csv_path) if args.output else default_json)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)
    logger.info("Wrote %s rows to %s", len(df), csv_path)
    logger.info("Wrote %s rows to %s", len(df), json_path)

    if df.empty:
        logger.warning("No signal history pairs could be compared.")
        return

    covered_pairs = {(row["workspace.name"], row["signal.objective"]) for row in results}
    missing_objectives: list[str] = []
    for ws in workspaces:
        for objective_id in {
            row["signal.objective"] for row in candidates_by_workspace.get(ws["id"], [])
        }:
            if (ws["name"], objective_id) not in covered_pairs:
                missing_objectives.append(f"{ws['name']}:{objective_id}")

    n_warnings = int(df["any.warning"].sum())
    logger.info(
        "Summary: comparisons=%s | customers=%s | objectives=%s | "
        "treatment warnings=%s | probability warnings=%s | any warning=%s",
        len(df),
        df["workspace.name"].nunique(),
        df["signal.objective"].nunique(),
        int(df["treatment.warning"].sum()),
        int(df["probability.warning"].sum()),
        n_warnings,
    )
    if missing_objectives:
        logger.warning(
            "Customer/objective pairs with no usable history: %s",
            ", ".join(sorted(missing_objectives)),
        )
    if n_warnings:
        logger.warning(
            "%s/%s signals exceeded the %.0f%% mismatch threshold.",
            n_warnings,
            len(df),
            MISMATCH_THRESHOLD * 100,
        )


if __name__ == "__main__":
    main()
