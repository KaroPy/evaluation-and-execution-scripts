"""
Calculate SHAP values for conversion / control / treatment models under ModelShapValues.

Discovers model-setting folders for a customer, loads each role model, explains a
feature matrix from --data-path, and writes CSVs plus summary plots.

Usage (from repo root):
    python scripts/ModelShapValues/calculate_shap_values.py \\
        --customer kfzteile24 \\
        --data-path /path/to/features.parquet \\
        --cv 6 \\
        --max-samples all

    python scripts/ModelShapValues/calculate_shap_values.py \\
        --customer tchibo \\
        --data-path /path/to/features.csv \\
        --setting 2026-09-12_best_models_with_landingpage_lstm
"""

from __future__ import annotations

import argparse
import logging
import pickle
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# shap 0.41 still references np.int, removed in NumPy 1.24+
if not hasattr(np, "int"):
    np.int = int  # type: ignore[attr-defined]

import pandas as pd
import shap
import xgboost as xgb
from xgboost import XGBRegressor

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MODELS_ROOT = SCRIPT_DIR / "models"
DEFAULT_OUTPUT_ROOT = SCRIPT_DIR / "shap_output"
DEFAULT_LOGS_DIR = SCRIPT_DIR / "logs"

ID_LIKE_COLUMNS = {
    "anonymousid",
    "anonymous_id",
    "userid",
    "user_id",
    "session",
    "sessionid",
    "session_id",
}
ROLE_PATTERNS = {
    "conversion": re.compile(r"(?:conversion_probability(?:_model)?\.h5$|conversion_probability$)"),
    "control": re.compile(r"_control(?:\.json|\.h5)$"),
    "treatment": re.compile(r"_treatment(?:\.json|\.h5)$"),
}
CV_RE = re.compile(r"_cv_(\d+)")
EXCLUDE_NAME_TOKENS = (
    "weights",
    "score",
    "feature_importance",
    "evals_result",
    "propensity",
    "model_mu_",
)


@dataclass(frozen=True)
class ModelArtifact:
    path: Path
    role: str
    cv: int
    backend: str  # "xgb" | "lstm"
    f1_score: float | None = None
    test_score_path: str | None = None


def model_family_prefix(model_path: Path) -> str | None:
    """Shared prefix that links a model file to its *_test_score.csv."""
    match = re.search(r"(.+_saved_model_cv_\d+)", model_path.name)
    return match.group(1) if match else None


def find_test_score_csv(setting_dir: Path, model_path: Path) -> Path | None:
    prefix = model_family_prefix(model_path)
    if not prefix:
        return None
    candidates = sorted(
        path
        for path in setting_dir.glob(f"{prefix}*_test_score.csv")
        if path.name.endswith("_test_score.csv") and "train_score" not in path.name
    )
    return candidates[0] if candidates else None


def read_f1_score(test_score_path: Path) -> float | None:
    try:
        frame = pd.read_csv(test_score_path)
    except Exception:
        return None
    if "F1" not in frame.columns or frame.empty:
        return None
    value = frame["F1"].iloc[0]
    if pd.isna(value):
        return None
    return float(value)


def select_best_artifacts_by_f1(
    artifacts: list[ModelArtifact],
    setting_dir: Path,
    logger: logging.Logger,
) -> list[ModelArtifact]:
    """
    When several models exist for the same role+cv (e.g. NNLSTM_50 vs NNLSTM_100),
    keep the family with the highest F1 from its *_test_score.csv.
    """
    grouped: dict[tuple[str, int], list[ModelArtifact]] = {}
    for artifact in artifacts:
        grouped.setdefault((artifact.role, artifact.cv), []).append(artifact)

    selected: list[ModelArtifact] = []
    for (role, cv), group in sorted(grouped.items()):
        scored: list[ModelArtifact] = []
        for artifact in group:
            score_path = find_test_score_csv(setting_dir, artifact.path)
            f1 = read_f1_score(score_path) if score_path else None
            scored.append(
                ModelArtifact(
                    path=artifact.path,
                    role=artifact.role,
                    cv=artifact.cv,
                    backend=artifact.backend,
                    f1_score=f1,
                    test_score_path=str(score_path) if score_path else None,
                )
            )

        if len(scored) == 1:
            selected.append(scored[0])
            continue

        scored.sort(
            key=lambda item: (
                item.f1_score is not None,
                item.f1_score if item.f1_score is not None else float("-inf"),
                item.path.name,
            ),
            reverse=True,
        )
        best = scored[0]
        logger.info(
            "  role=%s cv=%s: picked %s (F1=%s) from %s candidates via test_score.csv",
            role,
            cv,
            best.path.name,
            f"{best.f1_score:.6f}" if best.f1_score is not None else "n/a",
            len(scored),
        )
        for candidate in scored[1:]:
            logger.info(
                "    skipped %s (F1=%s)",
                candidate.path.name,
                f"{candidate.f1_score:.6f}" if candidate.f1_score is not None else "n/a",
            )
        selected.append(best)

    selected.sort(key=lambda item: (item.role, item.cv, item.path.name))
    return selected


def default_log_path(customer: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_customer = customer.strip().lower().replace(" ", "_")
    DEFAULT_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_LOGS_DIR / f"calculate_shap_values_{safe_customer}_{stamp}.log"


def setup_logging(verbose: bool, log_path: Path) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    logger = logging.getLogger("calculate_shap_values")
    logger.info("Logging to %s", log_path.resolve())
    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate SHAP values for conversion/control/treatment models "
            "under ModelShapValues for a given customer and feature data path."
        )
    )
    parser.add_argument(
        "--customer",
        required=True,
        help="Customer / workspace name, e.g. kfzteile24 or tchibo",
    )
    parser.add_argument(
        "--data-path",
        required=True,
        type=Path,
        help="Path to feature matrix (.parquet, .csv, or .pkl/.pickle)",
    )
    parser.add_argument(
        "--models-root",
        type=Path,
        default=DEFAULT_MODELS_ROOT,
        help=f"Root containing <customer>-aud-* folders (default: {DEFAULT_MODELS_ROOT})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=("Output directory (default: ModelShapValues/shap_output/<customer>/<data-filename>)"),
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help=(
            "Log file path (default: "
            "ModelShapValues/logs/calculate_shap_values_<customer>_<timestamp>.log)"
        ),
    )
    parser.add_argument(
        "--setting",
        action="append",
        default=None,
        help="Optional model-setting folder name to include (repeatable)",
    )
    parser.add_argument(
        "--cv",
        type=int,
        action="append",
        default=None,
        help="Optional CV fold(s) to include (repeatable). Default: all found folds.",
    )
    parser.add_argument(
        "--roles",
        nargs="+",
        default=["conversion", "control", "treatment"],
        choices=["conversion", "control", "treatment"],
        help="Model roles to explain",
    )
    parser.add_argument(
        "--max-samples",
        default="1000",
        help=(
            "Max rows from --data-path used for SHAP "
            "(integer, or 'all' for the complete dataset; default: 1000)"
        ),
    )
    parser.add_argument(
        "--background-samples",
        type=int,
        default=50,
        help="Background size for LSTM Gradient/Kernel explainer (default: 50)",
    )
    parser.add_argument(
        "--kernel-explain-samples",
        default="200",
        help=(
            "Max rows KernelExplainer explains when GradientExplainer fails "
            "(integer, or 'all' to use the full --max-samples subset; default: 200). "
            "Kernel SHAP is O(n·features·nsamples) — large values are slow."
        ),
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Sampling seed",
    )
    parser.add_argument(
        "--skip-lstm",
        action="store_true",
        help="Skip LSTM (.h5) models (XGB only)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Debug logging",
    )
    return parser.parse_args()


def resolve_customer_dirs(models_root: Path, customer: str) -> list[Path]:
    needle = customer.lower().replace(" ", "")
    matches = sorted(
        path
        for path in models_root.iterdir()
        if path.is_dir() and needle in path.name.lower().replace(" ", "")
    )
    if not matches:
        raise SystemExit(
            f"No customer folders matching '{customer}' under {models_root}. "
            f"Expected something like '{customer}-aud-<id>'."
        )
    return matches


def discover_settings(customer_dirs: list[Path], setting_filter: list[str] | None) -> list[Path]:
    settings: list[Path] = []
    for customer_dir in customer_dirs:
        for child in sorted(customer_dir.iterdir()):
            if not child.is_dir():
                continue
            if setting_filter and child.name not in setting_filter:
                continue
            settings.append(child)
    if not settings:
        raise SystemExit("No model-setting directories found for the given filters.")
    return settings


def detect_backend(path: Path) -> str:
    name = path.name.lower()
    if path.suffix == ".h5" or "nnlstm" in name:
        return "lstm"
    return "xgb"


def discover_artifacts(
    setting_dir: Path,
    roles: list[str],
    cvs: list[int] | None,
    skip_lstm: bool,
) -> list[ModelArtifact]:
    artifacts: list[ModelArtifact] = []
    for path in setting_dir.iterdir():
        if not path.is_file():
            continue
        name = path.name
        if any(token in name for token in EXCLUDE_NAME_TOKENS):
            continue
        matched_role = None
        for role, pattern in ROLE_PATTERNS.items():
            if role in roles and pattern.search(name):
                matched_role = role
                break
        if matched_role is None:
            continue
        cv_match = CV_RE.search(name)
        if not cv_match:
            continue
        cv = int(cv_match.group(1))
        if cvs is not None and cv not in cvs:
            continue
        backend = detect_backend(path)
        if skip_lstm and backend == "lstm":
            continue
        artifacts.append(ModelArtifact(path=path, role=matched_role, cv=cv, backend=backend))
    artifacts.sort(key=lambda item: (item.role, item.cv, item.path.name))
    return artifacts


def _is_parquet_file(path: Path) -> bool:
    """True if file starts with the Parquet magic bytes (PAR1)."""
    try:
        with path.open("rb") as handle:
            return handle.read(4) == b"PAR1"
    except OSError:
        return False


def load_feature_frame(data_path: Path) -> pd.DataFrame:
    if not data_path.exists():
        raise SystemExit(f"Data path does not exist: {data_path}")
    suffix = data_path.suffix.lower()

    # Some exports are Parquet content with a .csv suffix.
    if suffix == ".parquet" or _is_parquet_file(data_path):
        frame = pd.read_parquet(data_path)
    elif suffix == ".csv":
        try:
            frame = pd.read_csv(data_path)
        except UnicodeDecodeError as exc:
            raise SystemExit(
                f"Failed to read CSV at {data_path} as UTF-8. "
                "If this is a Parquet file with a .csv extension, rename it or "
                "ensure the file content matches the extension."
            ) from exc
    elif suffix in {".pkl", ".pickle"}:
        obj = pd.read_pickle(data_path)
        if not isinstance(obj, pd.DataFrame):
            raise SystemExit(f"Pickle at {data_path} is not a DataFrame.")
        frame = obj
    else:
        raise SystemExit(
            f"Unsupported data format '{suffix}'. Use .parquet, .csv, or .pkl/.pickle."
        )
    if frame.empty:
        raise SystemExit(f"Feature data is empty: {data_path}")
    return frame


def read_inputs_parquet(setting_dir: Path) -> list[str] | None:
    inputs_path = setting_dir / "inputs.parquet"
    if not inputs_path.exists():
        return None
    values = pd.read_parquet(inputs_path)["input"].dropna().astype(str).tolist()
    return [col for col in values if col.lower() not in ID_LIKE_COLUMNS]


def resolve_feature_columns(
    model,
    backend: str,
    setting_dir: Path,
    data_columns: list[str],
) -> list[str]:
    feature_names: list[str] | None = None
    source = None
    if backend == "xgb":
        booster = model.get_booster()
        if booster.feature_names:
            feature_names = list(booster.feature_names)
            source = "model.feature_names"
        elif hasattr(model, "feature_names_in_"):
            feature_names = list(model.feature_names_in_)
            source = "model.feature_names_in_"

    if not feature_names:
        feature_names = read_inputs_parquet(setting_dir)
        if feature_names:
            source = str(setting_dir / "inputs.parquet")

    if not feature_names:
        raise FileNotFoundError(
            f"Could not resolve model inputs for {setting_dir}. "
            "Expected either feature names on the model or an inputs.parquet "
            f"at {setting_dir / 'inputs.parquet'}."
        )

    missing = [col for col in feature_names if col not in data_columns]
    if missing:
        raise ValueError(
            f"Data is missing required feature columns from {source}: {missing}. "
            f"Available: {data_columns}"
        )
    return feature_names


def parse_sample_size(value: str | int | None, flag_name: str) -> int | None:
    """Return an int sample size, or None to use the full dataset."""
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"all", "*"}:
        return None
    try:
        size = int(text)
    except ValueError as exc:
        raise SystemExit(
            f"Invalid {flag_name} '{value}'. Use a positive integer or 'all'."
        ) from exc
    if size <= 0:
        raise SystemExit(f"{flag_name} must be a positive integer or 'all'.")
    return size


def parse_max_samples(value: str | int | None) -> int | None:
    return parse_sample_size(value, "--max-samples")


def sample_frame(frame: pd.DataFrame, max_samples: int | None, random_state: int) -> pd.DataFrame:
    if max_samples is None or len(frame) <= max_samples:
        return frame.copy()
    return frame.sample(n=max_samples, random_state=random_state).reset_index(drop=True)


def load_model(artifact: ModelArtifact):
    path = artifact.path
    if artifact.backend == "xgb":
        if path.suffix == ".json":
            model = XGBRegressor(enable_categorical=True)
            model.load_model(str(path))
            return model
        with path.open("rb") as handle:
            return pickle.load(handle)

    # LSTM / Keras
    from keras.models import load_model

    model = load_model(str(path), compile=False)
    weights = Path(str(path).replace(".h5", "_weights.weights.h5"))
    if not weights.exists():
        # common alternate naming
        weights = path.with_name(path.stem + "_weights.weights.h5")
    if weights.exists():
        model.load_weights(str(weights))
    return model


def prepare_x_for_model(
    frame: pd.DataFrame,
    feature_cols: list[str],
    backend: str,
) -> tuple[pd.DataFrame | np.ndarray, pd.DataFrame]:
    x_df = frame[feature_cols].copy()
    for col in x_df.columns:
        if pd.api.types.is_object_dtype(x_df[col]) or str(x_df[col].dtype) == "string":
            x_df[col] = x_df[col].astype("category")
        elif pd.api.types.is_bool_dtype(x_df[col]):
            x_df[col] = x_df[col].astype(int)

    if backend == "lstm":
        numeric = x_df.apply(pd.to_numeric, errors="coerce").fillna(0.0).astype(np.float32)
        # LSTM models expect (batch, timesteps, features)
        x_model = numeric.to_numpy().reshape(len(numeric), 1, numeric.shape[1])
        return x_model, numeric

    return x_df, x_df


def normalize_shap_values(shap_values) -> np.ndarray:
    """Return 2D array (n_samples, n_features)."""
    if isinstance(shap_values, list):
        # classifier: use positive class if available
        shap_values = shap_values[1] if len(shap_values) > 1 else shap_values[0]
    arr = np.asarray(shap_values)
    if arr.ndim == 3:
        # (n, features, classes) or similar -> take last class / squeeze
        arr = arr[:, :, -1]
    if arr.ndim != 2:
        raise ValueError(f"Unexpected SHAP value shape: {arr.shape}")
    return arr


def compute_shap_values(
    model,
    backend: str,
    x_model,
    x_plot: pd.DataFrame,
    background_samples: int,
    random_state: int,
    kernel_explain_samples: int | None = 200,
) -> np.ndarray:
    if backend == "xgb":
        # Prefer native XGBoost contributions (robust to shap/numpy version skew).
        booster = model.get_booster()
        if isinstance(x_model, pd.DataFrame):
            dmatrix = xgb.DMatrix(x_model, enable_categorical=True)
        else:
            dmatrix = xgb.DMatrix(x_model)
        contribs = booster.predict(dmatrix, pred_contribs=True)
        # Last column is bias; drop it for feature SHAP values.
        return np.asarray(contribs)[:, :-1]

    # LSTM: GradientExplainer with KernelExplainer fallback
    n_bg = min(background_samples, len(x_model))
    rng = np.random.default_rng(random_state)
    bg_idx = rng.choice(len(x_model), size=n_bg, replace=False)
    background = x_model[bg_idx]

    try:
        explainer = shap.GradientExplainer(model, background)
        values = explainer.shap_values(x_model)
        arr = normalize_shap_values(values)
        if arr.shape != (len(x_plot), x_plot.shape[1]):
            arr = np.asarray(values)
            if arr.ndim == 3 and arr.shape[1] == 1:
                arr = arr[:, 0, :]
            arr = normalize_shap_values(arr)
        return arr
    except Exception as gradient_err:
        logging.getLogger("calculate_shap_values").warning(
            "GradientExplainer failed (%s); falling back to KernelExplainer",
            gradient_err,
        )

        def predict_fn(data: np.ndarray) -> np.ndarray:
            if data.ndim == 2:
                data = data.reshape(data.shape[0], 1, data.shape[1])
            preds = model.predict(data, verbose=0)
            preds = np.asarray(preds).reshape(len(data), -1)
            return preds[:, -1]

        # KernelExplainer is expensive — optionally cap how many rows to explain
        if kernel_explain_samples is None:
            explain_n = len(x_plot)
        else:
            explain_n = min(len(x_plot), kernel_explain_samples)
        logging.getLogger("calculate_shap_values").info(
            "KernelExplainer explaining %s / %s rows (cap=%s)",
            explain_n,
            len(x_plot),
            "all" if kernel_explain_samples is None else kernel_explain_samples,
        )
        x_small = x_plot.iloc[:explain_n].to_numpy(dtype=np.float32)
        bg_2d = x_plot.iloc[bg_idx].to_numpy(dtype=np.float32)
        explainer = shap.KernelExplainer(predict_fn, bg_2d)
        values = explainer.shap_values(x_small, nsamples=100)
        return normalize_shap_values(values)


def short_setting_label(setting: str) -> str:
    """Shorten dated setting folder names for plot legends."""
    label = re.sub(r"^\d{4}-\d{2}-\d{2}_", "", setting)
    return label.replace("_", "\n") if len(label) > 40 else label


def plot_comparison_summary(
    comparison: pd.DataFrame,
    output_dir: Path,
    data_stem: str,
    customer: str,
) -> list[Path]:
    """
    Create summary plots across all model variations (settings).

    Writes:
      - comparison_mean_abs_shap_heatmap.png  (features × settings, one panel per role)
      - comparison_mean_abs_shap_bars.png     (grouped bars per role)
    """
    if comparison.empty:
        return []

    # Average across CVs when multiple folds were explained for the same setting.
    agg = comparison.groupby(["customer_setting", "role", "feature"], as_index=False)[
        "mean_abs_shap"
    ].mean()
    roles = [role for role in ["conversion", "control", "treatment"] if role in set(agg["role"])]
    settings = sorted(agg["customer_setting"].unique())
    features = (
        agg.groupby("feature")["mean_abs_shap"].mean().sort_values(ascending=False).index.tolist()
    )
    written: list[Path] = []

    # --- Heatmap: feature × setting, faceted by role ---
    n_roles = len(roles)
    fig_w = max(10.0, 1.6 * len(settings) + 4)
    fig_h = max(4.0, 0.35 * len(features) + 1.5) * n_roles
    fig, axes = plt.subplots(
        n_roles,
        1,
        figsize=(fig_w, fig_h),
        squeeze=False,
        constrained_layout=True,
    )
    for ax, role in zip(axes.flat, roles):
        pivot = (
            agg[agg["role"] == role]
            .pivot(index="feature", columns="customer_setting", values="mean_abs_shap")
            .reindex(index=features, columns=settings)
            .fillna(0.0)
        )
        im = ax.imshow(pivot.to_numpy(), aspect="auto", cmap="YlOrRd")
        ax.set_xticks(range(len(settings)))
        ax.set_xticklabels([short_setting_label(s) for s in settings], rotation=30, ha="right")
        ax.set_yticks(range(len(features)))
        ax.set_yticklabels(features)
        ax.set_xticks(np.arange(-0.5, len(settings), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(features), 1), minor=True)
        ax.grid(which="minor", color="white", linestyle="-", linewidth=1.0)
        ax.tick_params(which="minor", bottom=False, left=False)
        ax.set_title(f"{role}")
        fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, label="mean(|SHAP|)")
    fig.suptitle(
        f"{customer} — SHAP summary across model variations\ndata: {data_stem}",
        fontsize=13,
    )
    heatmap_path = output_dir / "comparison_mean_abs_shap_heatmap.png"
    fig.savefig(heatmap_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    written.append(heatmap_path)

    # --- Grouped horizontal bars: feature × setting, one panel per role ---
    fig_h = max(5.0, 0.45 * len(features) + 2.0) * n_roles
    fig, axes = plt.subplots(
        n_roles,
        1,
        figsize=(12, fig_h),
        squeeze=False,
        constrained_layout=True,
    )
    cmap = plt.get_cmap("tab10")
    colors = {setting: cmap(i % 10) for i, setting in enumerate(settings)}
    y = np.arange(len(features))
    n_settings = max(len(settings), 1)
    bar_height = min(0.8 / n_settings, 0.25)

    for ax, role in zip(axes.flat, roles):
        role_df = agg[agg["role"] == role]
        for i, setting in enumerate(settings):
            subset = (
                role_df[role_df["customer_setting"] == setting]
                .set_index("feature")
                .reindex(features)["mean_abs_shap"]
                .fillna(0.0)
            )
            offset = (i - (n_settings - 1) / 2) * bar_height
            ax.barh(
                y + offset,
                subset.to_numpy(),
                height=bar_height * 0.95,
                label=short_setting_label(setting).replace("\n", " "),
                color=colors[setting],
                alpha=0.9,
            )
        ax.set_yticks(y)
        ax.set_yticklabels(features)
        ax.invert_yaxis()
        ax.set_xlabel("mean(|SHAP|)")
        ax.set_title(f"{role}")
        ax.legend(loc="lower right", fontsize=8, framealpha=0.9)
        ax.grid(True, linestyle=":", alpha=0.5)
        ax.set_axisbelow(True)

    fig.suptitle(
        f"{customer} — mean(|SHAP|) by feature across model variations\ndata: {data_stem}",
        fontsize=13,
    )
    bars_path = output_dir / "comparison_mean_abs_shap_bars.png"
    fig.savefig(bars_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    written.append(bars_path)

    return written


def plot_beeswarm(
    shap_values: np.ndarray,
    features: pd.DataFrame,
    feature_names: list[str],
    title: str,
    output_path: Path,
    max_display: int = 20,
) -> None:
    """Matplotlib beeswarm that avoids shap/matplotlib colorbar incompatibilities."""
    mean_abs = np.abs(shap_values).mean(axis=0)
    order = np.argsort(mean_abs)[::-1][: min(max_display, len(feature_names))]
    order = order[::-1]  # lowest at bottom

    fig_h = max(4.0, 0.4 * len(order) + 1.5)
    fig, ax = plt.subplots(figsize=(10, fig_h))
    cmap = plt.get_cmap("coolwarm")

    for row_idx, feat_idx in enumerate(order):
        values = shap_values[:, feat_idx]
        raw = features.iloc[:, feat_idx].to_numpy()
        if np.issubdtype(np.asarray(raw).dtype, np.number):
            finite = np.isfinite(raw.astype(float))
            col = raw.astype(float).copy()
            if finite.any():
                lo, hi = np.nanpercentile(col[finite], [5, 95])
                if hi > lo:
                    col = np.clip((col - lo) / (hi - lo), 0, 1)
                else:
                    col = np.zeros_like(col)
            else:
                col = np.zeros_like(col, dtype=float)
        else:
            codes, _ = pd.factorize(raw, sort=True)
            col = codes / max(codes.max(), 1)

        jitter = np.random.default_rng(feat_idx).uniform(-0.18, 0.18, size=len(values))
        sc = ax.scatter(
            values,
            np.full(len(values), row_idx) + jitter,
            c=col,
            cmap=cmap,
            s=10,
            alpha=0.7,
            linewidths=0,
        )

    ax.set_yticks(range(len(order)))
    ax.set_yticklabels([feature_names[i] for i in order])
    ax.axvline(0, color="gray", linewidth=0.8)
    ax.set_xlabel("SHAP value (impact on model output)")
    ax.set_title(title)
    ax.grid(True, linestyle=":", alpha=0.5)
    ax.set_axisbelow(True)
    cbar = fig.colorbar(sc, ax=ax, aspect=40, pad=0.02)
    cbar.set_label("Feature value (low → high)")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def save_outputs(
    output_dir: Path,
    setting_name: str,
    artifact: ModelArtifact,
    feature_cols: list[str],
    x_plot: pd.DataFrame,
    shap_values: np.ndarray,
) -> dict:
    role_dir = output_dir / setting_name
    role_dir.mkdir(parents=True, exist_ok=True)
    stem = f"cv_{artifact.cv}_{artifact.role}"

    # Align lengths if KernelExplainer used a subset
    n = min(len(x_plot), len(shap_values))
    x_used = x_plot.iloc[:n].reset_index(drop=True)
    sv = shap_values[:n, : len(feature_cols)]

    shap_df = pd.DataFrame(sv, columns=feature_cols)
    shap_csv = role_dir / f"{stem}_shap_values.csv"
    shap_df.to_csv(shap_csv, index=False)

    mean_abs = (
        pd.DataFrame(
            {
                "feature": feature_cols,
                "mean_abs_shap": np.abs(sv).mean(axis=0),
                "mean_shap": sv.mean(axis=0),
            }
        )
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    mean_abs.insert(0, "customer_setting", setting_name)
    mean_abs.insert(1, "cv", artifact.cv)
    mean_abs.insert(2, "role", artifact.role)
    mean_abs.insert(3, "backend", artifact.backend)
    mean_abs.insert(4, "model_path", str(artifact.path))
    mean_csv = role_dir / f"{stem}_mean_abs_shap.csv"
    mean_abs.to_csv(mean_csv, index=False)

    beeswarm_path = role_dir / f"{stem}_shap_beeswarm.png"
    plot_beeswarm(
        sv,
        x_used,
        feature_cols,
        title=f"{setting_name}\n{stem} — SHAP summary",
        output_path=beeswarm_path,
        max_display=min(20, len(feature_cols)),
    )

    # Mean |SHAP| bar chart
    plt.figure(figsize=(10, max(3.5, 0.35 * len(mean_abs) + 1.5)))
    plot_df = mean_abs.sort_values("mean_abs_shap")
    plt.barh(plot_df["feature"], plot_df["mean_abs_shap"], color="#2a6f97")
    plt.xlabel("mean(|SHAP|)")
    plt.title(f"{setting_name}\n{stem} — feature importance (mean |SHAP|)")
    plt.grid(True, linestyle=":", alpha=0.5)
    ax = plt.gca()
    ax.set_axisbelow(True)
    plt.tight_layout()
    bar_path = role_dir / f"{stem}_shap_bar.png"
    plt.savefig(bar_path, dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "setting": setting_name,
        "cv": artifact.cv,
        "role": artifact.role,
        "backend": artifact.backend,
        "f1_score": artifact.f1_score,
        "test_score_path": artifact.test_score_path,
        "model_path": str(artifact.path),
        "n_rows": n,
        "shap_csv": str(shap_csv),
        "mean_abs_csv": str(mean_csv),
        "beeswarm_png": str(beeswarm_path),
        "bar_png": str(bar_path),
        "mean_abs": mean_abs,
    }


def process_setting(
    setting_dir: Path,
    data: pd.DataFrame,
    roles: list[str],
    cvs: list[int] | None,
    skip_lstm: bool,
    max_samples: int | None,
    background_samples: int,
    random_state: int,
    output_dir: Path,
    logger: logging.Logger,
    kernel_explain_samples: int | None = 200,
) -> list[dict]:
    artifacts = discover_artifacts(setting_dir, roles, cvs, skip_lstm)
    if not artifacts:
        logger.warning("No matching models in %s", setting_dir)
        return []

    artifacts = select_best_artifacts_by_f1(artifacts, setting_dir, logger)

    sampled = sample_frame(data, max_samples, random_state)
    results: list[dict] = []
    logger.info(
        "Setting %s: %s artifacts after F1 selection, explaining on %s rows",
        setting_dir.name,
        len(artifacts),
        len(sampled),
    )

    for artifact in artifacts:
        logger.info(
            "  explaining %s cv=%s (%s, F1=%s) <- %s",
            artifact.role,
            artifact.cv,
            artifact.backend,
            f"{artifact.f1_score:.6f}" if artifact.f1_score is not None else "n/a",
            artifact.path.name,
        )
        try:
            model = load_model(artifact)
            feature_cols = resolve_feature_columns(
                model, artifact.backend, setting_dir, list(sampled.columns)
            )
            x_model, x_plot = prepare_x_for_model(sampled, feature_cols, artifact.backend)
            shap_values = compute_shap_values(
                model,
                artifact.backend,
                x_model,
                x_plot,
                background_samples,
                random_state,
                kernel_explain_samples=kernel_explain_samples,
            )
            result = save_outputs(
                output_dir,
                setting_dir.name,
                artifact,
                feature_cols,
                x_plot if isinstance(x_plot, pd.DataFrame) else sampled[feature_cols],
                shap_values,
            )
            results.append(result)
            logger.info(
                "    wrote %s and %s",
                Path(result["mean_abs_csv"]).name,
                Path(result["beeswarm_png"]).name,
            )
        except Exception:
            logger.exception(
                "    failed for %s cv=%s (%s)",
                artifact.role,
                artifact.cv,
                artifact.path.name,
            )
    return results


def main() -> None:
    args = parse_args()
    customer = args.customer.strip()
    log_path = args.log_file.resolve() if args.log_file else default_log_path(customer)
    logger = setup_logging(args.verbose, log_path)

    models_root = args.models_root.resolve()
    data_path = args.data_path.resolve()
    data_stem = data_path.stem
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else (DEFAULT_OUTPUT_ROOT / customer.lower().replace(" ", "_") / data_stem).resolve()
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("customer=%s", customer)
    logger.info("data_path=%s", data_path)
    logger.info("models_root=%s", models_root)
    logger.info("output_dir=%s", output_dir)

    customer_dirs = resolve_customer_dirs(models_root, customer)
    logger.info("customer dirs: %s", [path.name for path in customer_dirs])
    settings = discover_settings(customer_dirs, args.setting)
    logger.info("settings: %s", [path.name for path in settings])

    data = load_feature_frame(data_path)
    max_samples = parse_max_samples(args.max_samples)
    kernel_explain_samples = parse_sample_size(
        args.kernel_explain_samples, "--kernel-explain-samples"
    )
    logger.info("loaded data shape=%s columns=%s", data.shape, list(data.columns))
    logger.info(
        "max_samples=%s",
        "all" if max_samples is None else max_samples,
    )
    logger.info(
        "kernel_explain_samples=%s",
        "all" if kernel_explain_samples is None else kernel_explain_samples,
    )

    all_results: list[dict] = []
    for setting_dir in settings:
        all_results.extend(
            process_setting(
                setting_dir=setting_dir,
                data=data,
                roles=args.roles,
                cvs=args.cv,
                skip_lstm=args.skip_lstm,
                max_samples=max_samples,
                background_samples=args.background_samples,
                random_state=args.random_state,
                output_dir=output_dir,
                logger=logger,
                kernel_explain_samples=kernel_explain_samples,
            )
        )

    if not all_results:
        raise SystemExit("No SHAP outputs were produced.")

    comparison = pd.concat([row["mean_abs"] for row in all_results], ignore_index=True)
    comparison_path = output_dir / "comparison_mean_abs_shap.csv"
    comparison.to_csv(comparison_path, index=False)

    summary_plots = plot_comparison_summary(
        comparison=comparison,
        output_dir=output_dir,
        data_stem=data_stem,
        customer=customer,
    )

    manifest = pd.DataFrame(
        [
            {
                "setting": row["setting"],
                "cv": row["cv"],
                "role": row["role"],
                "backend": row["backend"],
                "f1_score": row["f1_score"],
                "test_score_path": row["test_score_path"],
                "n_rows": row["n_rows"],
                "model_path": row["model_path"],
                "shap_csv": row["shap_csv"],
                "mean_abs_csv": row["mean_abs_csv"],
                "beeswarm_png": row["beeswarm_png"],
                "bar_png": row["bar_png"],
            }
            for row in all_results
        ]
    )
    manifest_path = output_dir / "shap_run_manifest.csv"
    manifest.to_csv(manifest_path, index=False)

    logger.info("Wrote comparison: %s", comparison_path)
    for path in summary_plots:
        logger.info("Wrote summary plot: %s", path)
    logger.info("Wrote manifest: %s", manifest_path)
    logger.info("Done. Explained %s model artifacts.", len(all_results))


if __name__ == "__main__":
    main()
