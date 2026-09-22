"""
Compare treatment/control user assignments between two targeting.history parquet files.

Downloads both files, labels control vs treatment via the `treatment` column,
counts users, and measures set overlap / differences for control and treatment groups.

Usage (from repo root):
    python scripts/PredictionComparison/compare_predictions.py

    python scripts/PredictionComparison/compare_predictions.py \\
        --label-a embedded \\
        --s3-a "s3://innkeepr-development/targeting.history/20260918/68b69dff2e5f2d9a58cce692_2026-09-18 10:01:50.363494_tchibo_embedded.parquet" \\
        --label-b standard \\
        --s3-b "s3://innkeepr-development/targeting.history/20260918/68b69dff2e5f2d9a58cce692_2026-09-18 11:32:27.527736_standard.parquet" \\
        --output-dir scripts/PredictionComparison/tchibo/20260918
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import awswrangler as wr
import boto3
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_LABEL_A = "embedded"
DEFAULT_S3_A = (
    "s3://innkeepr-development/targeting.history/20260918/"
    "68b69dff2e5f2d9a58cce692_2026-09-18 10:01:50.363494_tchibo_embedded.parquet"
)
DEFAULT_LABEL_B = "standard"
DEFAULT_S3_B = (
    "s3://innkeepr-development/targeting.history/20260918/"
    "68b69dff2e5f2d9a58cce692_2026-09-18 11:32:27.527736_standard.parquet"
)
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "tchibo" / "20260918"

USER_COL_CANDIDATES = ("anonymousId", "anonymous_id", "userId", "user_id")
CONTROL_TOKEN = "control"


def setup_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("compare_predictions")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    for noisy in ("boto3", "botocore", "s3transfer", "urllib3", "awswrangler"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)

    return logger


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def local_name_from_s3(s3_uri: str, label: str) -> str:
    key = parse_s3_uri(s3_uri)[1]
    suffix = Path(key).suffix or ".parquet"
    safe_stem = Path(key).name.replace(" ", "_").replace(":", "-")
    if not safe_stem.endswith(suffix):
        safe_stem = f"{safe_stem}{suffix}"
    return f"{label}__{safe_stem}"


def download_s3_file(s3_uri: str, dest: Path, logger: logging.Logger) -> Path:
    bucket, key = parse_s3_uri(s3_uri)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        logger.info("Reusing cached download: %s (%s bytes)", dest, dest.stat().st_size)
        return dest

    logger.info("Downloading s3://%s/%s -> %s", bucket, key, dest)
    boto3.client("s3").download_file(bucket, key, str(dest))
    logger.info("Downloaded %s bytes", dest.stat().st_size)
    return dest


def resolve_user_col(df: pd.DataFrame) -> str:
    for col in USER_COL_CANDIDATES:
        if col in df.columns:
            return col
    raise KeyError(
        f"No user id column found. Expected one of {USER_COL_CANDIDATES}; "
        f"got {list(df.columns)}"
    )


def load_parquet(path: Path, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Reading parquet: %s", path)
    df = pd.read_parquet(path)
    logger.info("Loaded shape=%s columns=%s", df.shape, list(df.columns))
    if "treatment" not in df.columns:
        raise KeyError(f"'treatment' column missing in {path}")
    return df


def classify_treatment_values(
    df: pd.DataFrame, label: str, logger: logging.Logger
) -> tuple[set[str], set[str]]:
    """Return (control_values, treatment_values) from the treatment column."""
    counts = df["treatment"].astype(str).value_counts(dropna=False)
    logger.info("[%s] treatment value counts:\n%s", label, counts.to_string())

    values = {str(v) for v in df["treatment"].dropna().unique()}
    control_values = {v for v in values if v.strip().lower() == CONTROL_TOKEN}
    treatment_values = values - control_values

    if not control_values:
        logger.warning("[%s] No '%s' value found in treatment column", label, CONTROL_TOKEN)
    if not treatment_values:
        logger.warning("[%s] No non-control treatment values found", label)

    logger.info("[%s] control values: %s", label, sorted(control_values))
    logger.info("[%s] treatment values: %s", label, sorted(treatment_values))
    return control_values, treatment_values


def user_sets(
    df: pd.DataFrame,
    user_col: str,
    control_values: set[str],
    treatment_values: set[str],
) -> tuple[set, set, set]:
    treatment_as_str = df["treatment"].astype(str)
    all_users = set(df[user_col].dropna().unique())
    control_users = set(df.loc[treatment_as_str.isin(control_values), user_col].dropna().unique())
    treatment_users = set(
        df.loc[treatment_as_str.isin(treatment_values), user_col].dropna().unique()
    )
    return all_users, control_users, treatment_users


def pct(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return round(100.0 * numerator / denominator, 4)


def set_diff_metrics(label_a: str, label_b: str, set_a: set, set_b: set) -> dict:
    only_a = set_a - set_b
    only_b = set_b - set_a
    intersection = set_a & set_b
    union = set_a | set_b
    return {
        f"users_{label_a}": len(set_a),
        f"users_{label_b}": len(set_b),
        "users_intersection": len(intersection),
        "users_union": len(union),
        f"only_in_{label_a}": len(only_a),
        f"only_in_{label_b}": len(only_b),
        "symmetric_difference": len(only_a) + len(only_b),
        f"only_in_{label_a}_pct_of_{label_a}": pct(len(only_a), len(set_a)),
        f"only_in_{label_b}_pct_of_{label_b}": pct(len(only_b), len(set_b)),
        "symmetric_difference_pct_of_union": pct(len(only_a) + len(only_b), len(union)),
        "same_users": set_a == set_b,
    }


def flatten_analysis_rows(
    label_a: str,
    label_b: str,
    all_a: set,
    all_b: set,
    ctrl_a: set,
    ctrl_b: set,
    treat_a: set,
    treat_b: set,
    control_values_a: set[str],
    control_values_b: set[str],
    treatment_values_a: set[str],
    treatment_values_b: set[str],
) -> list[dict]:
    rows: list[dict] = []

    def add(section: str, metric: str, value) -> None:
        rows.append({"section": section, "metric": metric, "value": value})

    add("treatment_labels", f"{label_a}_control_values", ",".join(sorted(control_values_a)))
    add("treatment_labels", f"{label_a}_treatment_values", ",".join(sorted(treatment_values_a)))
    add("treatment_labels", f"{label_b}_control_values", ",".join(sorted(control_values_b)))
    add("treatment_labels", f"{label_b}_treatment_values", ",".join(sorted(treatment_values_b)))

    add("overall_users", f"count_{label_a}", len(all_a))
    add("overall_users", f"count_{label_b}", len(all_b))
    add("overall_users", f"diff_{label_a}_minus_{label_b}", len(all_a) - len(all_b))
    add(
        "overall_users",
        f"diff_pct_of_{label_b}",
        pct(len(all_a) - len(all_b), len(all_b)),
    )
    for metric, value in set_diff_metrics(label_a, label_b, all_a, all_b).items():
        add("overall_users", metric, value)

    for metric, value in set_diff_metrics(label_a, label_b, ctrl_a, ctrl_b).items():
        add("control_group", metric, value)

    for metric, value in set_diff_metrics(label_a, label_b, treat_a, treat_b).items():
        add("treatment_group", metric, value)

    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label-a", default=DEFAULT_LABEL_A)
    parser.add_argument("--s3-a", default=DEFAULT_S3_A)
    parser.add_argument("--label-b", default=DEFAULT_LABEL_B)
    parser.add_argument("--s3-b", default=DEFAULT_S3_B)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for downloads, CSV analysis, and log file",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Read parquet directly from S3 via awswrangler (no local cache)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir if args.output_dir.is_absolute() else REPO_ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = output_dir / f"compare_predictions_{stamp}.log"
    csv_path = output_dir / f"prediction_comparison_{stamp}.csv"
    download_dir = output_dir / "downloads"

    logger = setup_logging(log_path)
    logger.info("Output dir: %s", output_dir)
    logger.info("Comparing %s vs %s", args.label_a, args.label_b)
    logger.info("S3 A (%s): %s", args.label_a, args.s3_a)
    logger.info("S3 B (%s): %s", args.label_b, args.s3_b)

    if args.skip_download:
        logger.info("Reading parquet from S3: %s", args.s3_a)
        df_a = wr.s3.read_parquet(args.s3_a)
        logger.info("[%s] shape=%s columns=%s", args.label_a, df_a.shape, list(df_a.columns))
        logger.info("Reading parquet from S3: %s", args.s3_b)
        df_b = wr.s3.read_parquet(args.s3_b)
        logger.info("[%s] shape=%s columns=%s", args.label_b, df_b.shape, list(df_b.columns))
    else:
        path_a = download_s3_file(
            args.s3_a, download_dir / local_name_from_s3(args.s3_a, args.label_a), logger
        )
        path_b = download_s3_file(
            args.s3_b, download_dir / local_name_from_s3(args.s3_b, args.label_b), logger
        )
        df_a = load_parquet(path_a, logger)
        df_b = load_parquet(path_b, logger)

    if "treatment" not in df_a.columns or "treatment" not in df_b.columns:
        raise KeyError("Both files must contain a 'treatment' column")

    user_col_a = resolve_user_col(df_a)
    user_col_b = resolve_user_col(df_b)
    if user_col_a != user_col_b:
        logger.warning("User columns differ: %s vs %s — comparing as user ids anyway", user_col_a, user_col_b)
    logger.info("User column: %s / %s", user_col_a, user_col_b)

    ctrl_vals_a, treat_vals_a = classify_treatment_values(df_a, args.label_a, logger)
    ctrl_vals_b, treat_vals_b = classify_treatment_values(df_b, args.label_b, logger)

    all_a, ctrl_a, treat_a = user_sets(df_a, user_col_a, ctrl_vals_a, treat_vals_a)
    all_b, ctrl_b, treat_b = user_sets(df_b, user_col_b, ctrl_vals_b, treat_vals_b)

    logger.info("--- Overall user counts ---")
    logger.info("%s: %s users", args.label_a, len(all_a))
    logger.info("%s: %s users", args.label_b, len(all_b))
    logger.info(
        "Difference (%s - %s): %s (%.4f%% of %s)",
        args.label_a,
        args.label_b,
        len(all_a) - len(all_b),
        pct(len(all_a) - len(all_b), len(all_b)) or 0.0,
        args.label_b,
    )

    for group_name, set_a, set_b in (
        ("control", ctrl_a, ctrl_b),
        ("treatment", treat_a, treat_b),
    ):
        metrics = set_diff_metrics(args.label_a, args.label_b, set_a, set_b)
        logger.info("--- %s group ---", group_name)
        for key, value in metrics.items():
            logger.info("  %s: %s", key, value)

    rows = flatten_analysis_rows(
        args.label_a,
        args.label_b,
        all_a,
        all_b,
        ctrl_a,
        ctrl_b,
        treat_a,
        treat_b,
        ctrl_vals_a,
        ctrl_vals_b,
        treat_vals_a,
        treat_vals_b,
    )
    analysis = pd.DataFrame(rows)
    analysis.to_csv(csv_path, index=False)
    logger.info("Wrote analysis CSV: %s", csv_path)
    logger.info("Wrote log: %s", log_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
