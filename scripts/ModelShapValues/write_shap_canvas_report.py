"""
Generate a Cursor canvas (.canvas.tsx) SHAP summary report from shap_output results.

Reads ``comparison_mean_abs_shap.csv`` when present, otherwise aggregates
``**/cv_*_*_mean_abs_shap.csv`` under the given results directory. Optionally
uses ``shap_run_manifest.csv`` for sample sizes.

Usage (from repo root):
    python scripts/ModelShapValues/write_shap_canvas_report.py \\
        --shap-results scripts/ModelShapValues/shap_output/kfzteile24/kfzteile24_6835d77813526dc3d1229978_test_data

    python scripts/ModelShapValues/write_shap_canvas_report.py \\
        --shap-results /abs/path/to/shap_output/<customer>/<data_stem> \\
        --output /Users/<you>/.cursor/projects/<workspace>/canvases/my-report.canvas.tsx \\
        --title "kfzteile24 SHAP evaluation" \\
        --pdf
"""

from __future__ import annotations

import argparse
import json
import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
MEAN_ABS_GLOB = "**/cv_*_*_mean_abs_shap.csv"
COMPARISON_NAME = "comparison_mean_abs_shap.csv"
MANIFEST_NAME = "shap_run_manifest.csv"
ROLES = ("control", "treatment", "conversion")
BUCKETS = ("pages", "duration", "landing", "recency", "other")
BUCKET_COLORS = {
    "pages": "#3B82F6",
    "duration": "#6B7280",
    "landing": "#10B981",
    "recency": "#F59E0B",
    "other": "#EF4444",
}
BUCKET_LABELS = {
    "pages": "Pages visited",
    "duration": "Session duration",
    "landing": "Landing",
    "recency": "Recency",
    "other": "Other",
}


@dataclass
class ReportData:
    title: str
    source_label: str
    customer_hint: str
    settings: list[str]
    setting_scope_rows: list[list[str]]
    sample_n_by_setting: dict[str, int]
    bucket_shares: dict[str, dict[str, list[float]]]
    top1_rows: list[list[str]]
    landing_rows: list[list[str]]
    control_features: list[str]
    control_series: list[dict[str, Any]]
    stats: dict[str, str]
    findings: list[tuple[str, str]]
    takeaway: str
    footnotes: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a Cursor canvas SHAP summary report from shap_output results."
    )
    parser.add_argument(
        "--shap-results",
        required=True,
        type=Path,
        help="Directory with comparison_mean_abs_shap.csv and/or per-setting mean_abs CSVs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Destination .canvas.tsx path. Defaults to the Cursor workspace canvases/ "
            "folder when detectable, else <shap-results>/<stem>_shap_summary.canvas.tsx."
        ),
    )
    parser.add_argument(
        "--title",
        type=str,
        default=None,
        help="Report title override (default derived from path).",
    )
    parser.add_argument(
        "--cv",
        type=int,
        default=None,
        help="If set, keep only this CV fold.",
    )
    parser.add_argument(
        "--pdf",
        action="store_true",
        help="Also write a multi-page PDF next to the canvas (or to --pdf-output).",
    )
    parser.add_argument(
        "--pdf-output",
        type=Path,
        default=None,
        help="Explicit PDF path. Defaults to <shap-results>/<stem>_shap_summary.pdf.",
    )
    parser.add_argument(
        "--pdf-only",
        action="store_true",
        help="Write PDF only (skip .canvas.tsx).",
    )
    parser.add_argument(
        "--html",
        action="store_true",
        help="Also write a standalone HTML report (or to --html-output).",
    )
    parser.add_argument(
        "--html-output",
        type=Path,
        default=None,
        help="Explicit HTML path. Defaults to <shap-results>/<stem>_shap_summary.html.",
    )
    parser.add_argument(
        "--html-only",
        action="store_true",
        help="Write HTML only (skip .canvas.tsx).",
    )
    return parser.parse_args()


def default_canvases_dir() -> Path | None:
    """Best-effort Cursor managed canvases directory for this repo."""
    home = Path.home()
    projects = home / ".cursor" / "projects"
    if not projects.is_dir():
        return None
    # Prefer exact workspace slug matching this repo path.
    slug = str(REPO_ROOT).lstrip("/").replace("/", "-")
    candidate = projects / slug / "canvases"
    if candidate.is_dir():
        return candidate
    # Fallback: any project folder whose name ends with this repo name.
    matches = sorted(projects.glob(f"*{REPO_ROOT.name}/canvases"))
    return matches[0] if matches else None


def resolve_output_path(shap_results: Path, output: Path | None) -> Path:
    if output is not None:
        path = output.expanduser().resolve()
        if path.suffix != ".tsx":
            path = path.with_suffix(".canvas.tsx") if path.suffix == "" else path
        if not path.name.endswith(".canvas.tsx"):
            path = path.with_name(path.stem + ".canvas.tsx")
        return path

    canvases = default_canvases_dir()
    customer = shap_results.parent.name
    stem = shap_results.name
    # Avoid customer-customer-... when the data stem already starts with the customer name.
    if stem.startswith(f"{customer}_") or stem.startswith(f"{customer}-"):
        name = f"{_slugify(stem)}-shap-summary.canvas.tsx"
    else:
        name = f"{_slugify(customer)}-{_slugify(stem)}-shap-summary.canvas.tsx"
    if canvases is not None:
        return canvases / name
    return (shap_results / name).resolve()


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def short_setting_label(setting: str) -> str:
    """Human-readable short label from a setting folder name (keeps date)."""
    date_match = re.match(r"^(\d{4}-\d{2}-\d{2})_(.*)$", setting)
    date_prefix = date_match.group(1) if date_match else None
    s = date_match.group(2) if date_match else setting
    parts: list[str] = []
    if "embed" in s:
        parts.append("LSTM emb")
    elif "lstm" in s:
        parts.append("LSTM")
    elif "xgb" in s:
        parts.append("XGB")
    if "best_models" in s:
        parts.append("best")
    elif "new_models" in s:
        parts.append("new")
    if "permutat" in s:
        parts.append("LP permutated")
    elif "without_landingpage" in s or "no_landing" in s:
        parts.append("no LP")
    elif "with_landingpage" in s or "landingpage" in s or "embed" in s:
        parts.append("+ LP")
    if "standard" in s:
        parts.append("standard")
    if "reset" in s:
        parts.append("reset")
    if "failed" in s:
        parts.append("failed")
    body = " ".join(parts) if parts else (setting[-40:] if len(setting) > 40 else setting)
    if date_prefix:
        return f"{date_prefix} {body}"
    return body


def _read_f1_from_test_score(path: Path) -> float | None:
    try:
        frame = pd.read_csv(path)
    except Exception:
        return None
    if "F1" not in frame.columns or frame.empty:
        return None
    value = frame["F1"].iloc[0]
    if pd.isna(value):
        return None
    return float(value)


def resolve_setting_f1(
    setting: str,
    shap_results: Path,
    df: pd.DataFrame,
    cv: int | None = None,
) -> float | None:
    """Best-effort F1 from model_path / setting folder *_test_score.csv."""
    sub = df[df["customer_setting"] == setting]
    if cv is not None:
        sub = sub[sub["cv"] == cv]
    cvs = sorted({int(c) for c in sub["cv"].dropna().unique() if int(c) >= 0})
    prefer_cv = cvs[0] if len(cvs) == 1 else (cv if cv is not None else (cvs[-1] if cvs else None))

    model_paths = [
        Path(p)
        for p in sub["model_path"].fillna("").astype(str).unique()
        if p and p != "nan"
    ]
    setting_dirs: list[Path] = []
    for mp in model_paths:
        if mp.parent.is_dir() and mp.parent not in setting_dirs:
            setting_dirs.append(mp.parent)
    local_dir = shap_results / setting
    # Prefer the models/... sibling directory inferred from model_path.
    candidates: list[Path] = []
    for sdir in setting_dirs or ([local_dir] if local_dir.is_dir() else []):
        for path in sdir.glob("*_test_score.csv"):
            if path.name.endswith("_score_test.csv"):
                continue
            if prefer_cv is not None and not re.search(rf"_cv_{prefer_cv}(?:_|\.|$)", path.name):
                continue
            candidates.append(path)
    scores = [_read_f1_from_test_score(p) for p in candidates]
    scores = [s for s in scores if s is not None]
    return max(scores) if scores else None


def resolve_setting_sample_n(setting: str, shap_results: Path, df: pd.DataFrame) -> int | None:
    """Sample n from SHAP value CSV row count, else None."""
    setting_dir = shap_results / setting
    if setting_dir.is_dir():
        shap_csvs = sorted(setting_dir.glob("cv_*_*_shap_values.csv"))
        for path in shap_csvs:
            try:
                # Fast path: count data lines without loading full frame when huge.
                with path.open("r", encoding="utf-8") as handle:
                    n = sum(1 for _ in handle) - 1
                if n > 0:
                    return int(n)
            except Exception:
                continue
    # Fallback: unique rows are not in mean_abs; leave unknown.
    _ = df
    return None


def feature_bucket(feature: str) -> str:
    f = feature.lower()
    if "landing" in f:
        return "landing"
    if f == "days_between_sessions" or "recency" in f or f.startswith("days_"):
        return "recency"
    if "session_duration" in f or f.endswith("_duration") or "duration_in_s" in f:
        return "duration"
    if "pages_visited" in f or "page" in f:
        return "pages"
    return "other"


def model_priority(model_path: str) -> tuple[int, str]:
    """Prefer a single primary artifact when multiple LSTM sizes exist."""
    p = str(model_path)
    # Prefer NNLSTM_50 over _100 when both appear in a comparison dump.
    if "NNLSTM_50" in p:
        return (0, p)
    if "NNLSTM_100" in p:
        return (1, p)
    return (0, p)


def load_shap_frame(shap_results: Path, cv: int | None) -> pd.DataFrame:
    shap_results = shap_results.expanduser().resolve()
    if not shap_results.is_dir():
        raise SystemExit(f"SHAP results path is not a directory: {shap_results}")

    comparison = shap_results / COMPARISON_NAME
    if comparison.is_file():
        df = pd.read_csv(comparison)
        source = comparison.name
    else:
        files = sorted(shap_results.glob(MEAN_ABS_GLOB))
        if not files:
            raise SystemExit(
                f"No {COMPARISON_NAME} or {MEAN_ABS_GLOB} under {shap_results}"
            )
        frames = [pd.read_csv(f) for f in files]
        df = pd.concat(frames, ignore_index=True)
        source = f"{len(files)} mean_abs CSV(s)"

    required = {"customer_setting", "role", "feature", "mean_abs_shap"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns in SHAP table: {sorted(missing)}")

    if "mean_shap" not in df.columns:
        df["mean_shap"] = float("nan")
    if "backend" not in df.columns:
        df["backend"] = ""
    if "model_path" not in df.columns:
        df["model_path"] = ""
    if "cv" not in df.columns:
        df["cv"] = -1

    if cv is not None:
        df = df[df["cv"] == cv].copy()
        if df.empty:
            raise SystemExit(f"No rows for --cv {cv}")

    df["customer_setting"] = df["customer_setting"].astype(str)
    df["role"] = df["role"].astype(str).str.lower()
    df["feature"] = df["feature"].astype(str)
    df["mean_abs_shap"] = pd.to_numeric(df["mean_abs_shap"], errors="coerce").fillna(0.0)
    df["mean_shap"] = pd.to_numeric(df["mean_shap"], errors="coerce")
    df["setting_label"] = df["customer_setting"].map(short_setting_label)
    df["_src"] = source
    return df


def load_manifest(shap_results: Path) -> pd.DataFrame | None:
    path = shap_results.expanduser().resolve() / MANIFEST_NAME
    if not path.is_file():
        return None
    return pd.read_csv(path)


def dedupe_primary_models(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one model_path per setting × role (prefer LSTM_50)."""
    if df.empty:
        return df
    keys = ["customer_setting", "role"]
    if "cv" in df.columns:
        keys = ["customer_setting", "cv", "role"]

    preferred: list[pd.DataFrame] = []
    for _, group in df.groupby(keys, sort=False):
        paths = group["model_path"].fillna("").astype(str).unique().tolist()
        if len(paths) <= 1:
            preferred.append(group)
            continue
        best = sorted(paths, key=model_priority)[0]
        preferred.append(group[group["model_path"].astype(str) == best])
    return pd.concat(preferred, ignore_index=True)


def _pct(part: float, total: float) -> float:
    return round(100.0 * part / total, 1) if total > 0 else 0.0


def infer_title(shap_results: Path, title: str | None) -> tuple[str, str]:
    if title:
        return title, shap_results.name
    customer = shap_results.parent.name if shap_results.parent.name else "customer"
    stem = shap_results.name
    return f"{customer} SHAP evaluation", stem


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def build_report(df: pd.DataFrame, shap_results: Path, title: str | None, manifest: pd.DataFrame | None) -> ReportData:
    df = dedupe_primary_models(df)
    report_title, source_stem = infer_title(shap_results, title)
    source_label = f"{_display_path(shap_results)} · {df['_src'].iloc[0]}"

    # Stable setting order: by original folder name.
    setting_order = sorted(df["customer_setting"].unique().tolist())
    labels = [short_setting_label(s) for s in setting_order]
    # Disambiguate duplicate short labels.
    seen: dict[str, int] = {}
    uniq_labels: list[str] = []
    label_of: dict[str, str] = {}
    for setting, label in zip(setting_order, labels):
        n = seen.get(label, 0)
        seen[label] = n + 1
        final = label if n == 0 else f"{label} ({n + 1})"
        # If collision likely, include truncated date/suffix.
        if labels.count(label) > 1:
            final = f"{label} · {setting[:10]}"
        uniq_labels.append(final)
        label_of[setting] = final
    # Recompute if we used date disambiguation uniformly when any collision.
    if len(set(labels)) < len(labels):
        uniq_labels = []
        label_of = {}
        for setting in setting_order:
            base = short_setting_label(setting)
            final = f"{base} · {setting[:10]}"
            uniq_labels.append(final)
            label_of[setting] = final

    df = df.copy()
    df["setting_label"] = df["customer_setting"].map(label_of)

    sample_n_by_setting: dict[str, int] = {}
    f1_by_setting: dict[str, float] = {}
    if manifest is not None:
        m = manifest.copy()
        setting_col = "setting" if "setting" in m.columns else "customer_setting"
        if setting_col in m.columns:
            for setting, g in m.groupby(setting_col):
                if setting not in label_of:
                    continue
                label = label_of[str(setting)]
                if "n_rows" in g.columns:
                    sample_n_by_setting[label] = int(g["n_rows"].iloc[0])
                if "f1_score" in g.columns:
                    f1_vals = pd.to_numeric(g["f1_score"], errors="coerce").dropna()
                    if not f1_vals.empty:
                        # Manifest stores the setting's selected-model F1 (same across roles).
                        f1_by_setting[label] = float(f1_vals.max())

    # Fill gaps from model *_test_score.csv and SHAP value CSV row counts.
    for setting in setting_order:
        label = label_of[setting]
        if label not in f1_by_setting:
            cvs = sorted(
                {
                    int(c)
                    for c in df.loc[df["customer_setting"] == setting, "cv"].dropna().unique()
                    if int(c) >= 0
                }
            )
            prefer_cv = cvs[0] if len(cvs) == 1 else None
            f1 = resolve_setting_f1(setting, shap_results, df, cv=prefer_cv)
            if f1 is not None:
                f1_by_setting[label] = f1
        if label not in sample_n_by_setting:
            n = resolve_setting_sample_n(setting, shap_results, df)
            if n is not None:
                sample_n_by_setting[label] = n

    setting_scope_rows: list[list[str]] = []
    for setting in setting_order:
        label = label_of[setting]
        sub = df[df["customer_setting"] == setting]
        backends = sorted({str(b) for b in sub["backend"].dropna().unique() if str(b)})
        feats = sorted(sub["feature"].unique().tolist())
        landing = [f for f in feats if "landing" in f.lower()]
        n = sample_n_by_setting.get(label, -1)
        f1 = f1_by_setting.get(label)
        f1_cell = f"{f1:.4f}" if f1 is not None else "—"
        setting_scope_rows.append(
            [
                label,
                ", ".join(backends) or "—",
                ", ".join(landing) if landing else "—",
                str(n) if n > 0 else "—",
                f1_cell,
            ]
        )

    # Bucket shares per role, aligned to setting order.
    bucket_shares: dict[str, dict[str, list[float]]] = {}
    for role in ROLES:
        shares = {b: [] for b in BUCKETS}
        for setting in setting_order:
            g = df[(df["customer_setting"] == setting) & (df["role"] == role)]
            total = float(g["mean_abs_shap"].sum())
            for b in BUCKETS:
                part = float(g.loc[g["feature"].map(feature_bucket) == b, "mean_abs_shap"].sum())
                shares[b].append(_pct(part, total))
        bucket_shares[role] = shares

    # Top-1 feature per setting × role, with sample n + best F1.
    top1_rows: list[list[str]] = []
    for setting in setting_order:
        label = label_of[setting]
        f1 = f1_by_setting.get(label)
        f1_cell = f"{f1:.4f}" if f1 is not None else "—"
        n = sample_n_by_setting.get(label, -1)
        n_cell = str(n) if n > 0 else "—"
        row = [label, n_cell, f1_cell]
        for role in ROLES:
            g = df[(df["customer_setting"] == setting) & (df["role"] == role)]
            if g.empty:
                row.append("—")
                continue
            top = g.sort_values("mean_abs_shap", ascending=False).iloc[0]
            row.append(str(top["feature"]))
        top1_rows.append(row)

    # Landing feature table.
    landing_rows: list[list[str]] = []
    for setting in setting_order:
        g_all = df[df["customer_setting"] == setting]
        landing_feats = sorted({f for f in g_all["feature"] if "landing" in f.lower()})
        if not landing_feats:
            continue
        for feat in landing_feats:
            cells = [label_of[setting], feat]
            for role in ROLES:
                g = df[(df["customer_setting"] == setting) & (df["role"] == role)].sort_values(
                    "mean_abs_shap", ascending=False
                )
                if g.empty or feat not in set(g["feature"]):
                    cells.append("—")
                    continue
                ranks = {f: i + 1 for i, f in enumerate(g["feature"])}
                total = float(g["mean_abs_shap"].sum())
                val = float(g.loc[g["feature"] == feat, "mean_abs_shap"].iloc[0])
                cells.append(f"rank {ranks[feat]}/{len(g)} · {_pct(val, total)}%")
            landing_rows.append(cells)

    # Control mean |SHAP| chart: union of top features across settings (cap 8).
    control = df[df["role"] == "control"]
    feature_totals = (
        control.groupby("feature")["mean_abs_shap"].mean().sort_values(ascending=False)
    )
    control_features = feature_totals.head(8).index.tolist()
    # Ensure landing features appear if present anywhere in control.
    for f in sorted({x for x in control["feature"] if "landing" in x.lower()}):
        if f not in control_features:
            control_features.append(f)
    control_series: list[dict[str, Any]] = []
    tones = ["info", "neutral", "success", "warning", "danger"]
    for i, setting in enumerate(setting_order):
        g = control[control["customer_setting"] == setting].set_index("feature")
        data = [
            float(g.loc[f, "mean_abs_shap"]) if f in g.index else 0.0 for f in control_features
        ]
        control_series.append(
            {
                "name": label_of[setting],
                "data": data,
                "tone": tones[i % len(tones)],
            }
        )

    # Stats + findings.
    pages_control = [
        bucket_shares["control"]["pages"][i] for i in range(len(setting_order))
    ]
    pages_lo = min(pages_control) if pages_control else 0
    pages_hi = max(pages_control) if pages_control else 0

    landing_control_shares: list[float] = []
    for setting in setting_order:
        g = df[(df["customer_setting"] == setting) & (df["role"] == "control")]
        total = float(g["mean_abs_shap"].sum())
        part = float(g.loc[g["feature"].map(feature_bucket) == "landing", "mean_abs_shap"].sum())
        if part > 0:
            landing_control_shares.append(_pct(part, total))
    best_landing = max(landing_control_shares) if landing_control_shares else 0.0
    weak_landing = min(landing_control_shares) if landing_control_shares else 0.0

    # Degenerate: treatment total |SHAP| much smaller than peer median.
    treatment_totals = []
    for setting in setting_order:
        total = float(
            df[(df["customer_setting"] == setting) & (df["role"] == "treatment")][
                "mean_abs_shap"
            ].sum()
        )
        treatment_totals.append((label_of[setting], total))
    degenerate = ""
    if treatment_totals:
        vals = [t for _, t in treatment_totals]
        med = sorted(vals)[len(vals) // 2]
        for label, total in treatment_totals:
            if med > 0 and total < 0.15 * med:
                degenerate = label
                break

    stats = {
        "pages_range": f"{pages_lo:.0f}–{pages_hi:.0f}%",
        "best_landing": f"{best_landing:.0f}%" if landing_control_shares else "n/a",
        "weak_landing": f"{weak_landing:.0f}%" if len(landing_control_shares) > 1 else (
            f"{weak_landing:.0f}%" if landing_control_shares else "n/a"
        ),
        "degenerate": degenerate or "none",
    }

    findings: list[tuple[str, str]] = []
    findings.append(
        (
            "Engagement is the backbone",
            (
                f"Page-visit features account for about {pages_lo:.0f}–{pages_hi:.0f}% of "
                "control mean |SHAP| across settings. Models primarily read browsing intensity."
            ),
        )
    )
    if landing_rows:
        findings.append(
            (
                "Landing-page signal is setting-dependent",
                (
                    "When a landing feature is present, its control share ranges from "
                    f"{weak_landing:.0f}% to {best_landing:.0f}% of total mean |SHAP|. "
                    "Without it, that budget is absorbed by page-count / duration features."
                ),
            )
        )
    else:
        findings.append(
            (
                "No landing feature in this run",
                "None of the settings exposed a landingpage / landingpage_cat feature in the SHAP tables.",
            )
        )

    # LSTM vs XGB history note when both backends present.
    backends = set(df["backend"].astype(str).str.lower())
    if "lstm" in backends and "xgb" in backends:
        findings.append(
            (
                "LSTM vs XGB use history differently",
                (
                    "Compare top-1 features: LSTM often elevates cumulated page counts; "
                    "XGB more often prefers spot counts (pages_visited_per_session / _per_user). "
                    "Use within-model shares — absolute |SHAP| is not comparable across backends."
                ),
            )
        )

    # Recency on conversion.
    recency_conv = bucket_shares["conversion"]["recency"]
    if recency_conv and max(recency_conv) >= 15:
        idx = int(max(range(len(recency_conv)), key=lambda i: recency_conv[i]))
        findings.append(
            (
                "Recency can matter for conversion",
                (
                    f"{uniq_labels[idx]} puts recency at {recency_conv[idx]:.0f}% of conversion "
                    "|SHAP|. Check days_between_sessions before discarding it."
                ),
            )
        )

    if degenerate:
        findings.append(
            (
                f"Possible degenerate treatment model: {degenerate}",
                (
                    "Treatment total mean |SHAP| is far below other settings. Inspect whether "
                    "the model under-fits or SHAP sampling hit a flat region; cumulated features "
                    "at exactly 0 are a red flag."
                ),
            )
        )

    findings.append(
        (
            "Do not compare raw |SHAP| across roles/backends",
            (
                "Conversion heads (especially XGB) often have much larger absolute SHAP "
                "totals than control/treatment or LSTM probability outputs. Compare ranks "
                "or within-model percentage shares only."
            ),
        )
    )

    takeaway = (
        f"For {report_title.split(' SHAP')[0]}, lead interpretation with page-visit / "
        "session-duration features. Treat landing features as high-value when they rank "
        "in the top half for control/treatment; otherwise keep them as secondary context. "
        "Prefer relative feature shares when comparing settings."
    )

    cvs = sorted({int(c) for c in df["cv"].dropna().unique() if int(c) >= 0})
    footnotes = [
        f"Source: {source_label}",
        f"CV folds: {', '.join(map(str, cvs)) if cvs else 'unspecified'}",
        f"Data stem: {source_stem}",
    ]

    return ReportData(
        title=report_title,
        source_label=source_label,
        customer_hint=shap_results.parent.name,
        settings=uniq_labels,
        setting_scope_rows=setting_scope_rows,
        sample_n_by_setting=sample_n_by_setting,
        bucket_shares=bucket_shares,
        top1_rows=top1_rows,
        landing_rows=landing_rows,
        control_features=control_features,
        control_series=control_series,
        stats=stats,
        findings=findings,
        takeaway=takeaway,
        footnotes=footnotes,
    )


def _tsx_str(value: str) -> str:
    """JSON-encode a string for embedding in TS/JSX."""
    return json.dumps(value, ensure_ascii=False)


def _tsx_list_of_lists(rows: list[list[str]]) -> str:
    inner = ",\n  ".join(
        "[" + ", ".join(_tsx_str(c) for c in row) + "]" for row in rows
    )
    return "[\n  " + inner + ",\n]" if rows else "[]"


def _tsx_number_list(vals: list[float]) -> str:
    return "[" + ", ".join(f"{v:.1f}" for v in vals) + "]"


def _jsx_text(value: str) -> str:
    """Embed a Python string as a JSX expression child/prop value."""
    return "{" + _tsx_str(value) + "}"


def render_canvas(report: ReportData) -> str:
    settings_literal = ",\n  ".join(_tsx_str(s) for s in report.settings)
    roles_with_data = [
        r for r in ROLES if any(report.bucket_shares[r][b] for b in BUCKETS)
    ]

    bucket_block_lines = ["const BUCKET_SHARES = {"]
    for role in roles_with_data:
        bucket_block_lines.append(f"  {role}: {{")
        for b in BUCKETS:
            if b == "other" and all(v == 0 for v in report.bucket_shares[role][b]):
                continue
            bucket_block_lines.append(
                f"    {b}: {_tsx_number_list(report.bucket_shares[role][b])},"
            )
        bucket_block_lines.append("  },")
    bucket_block_lines.append("} as const;")
    bucket_block = "\n".join(bucket_block_lines)

    bucket_labels = {
        "pages": "Pages visited",
        "duration": "Session duration",
        "landing": "Landing",
        "recency": "Recency",
        "other": "Other",
    }
    bucket_tones = {
        "pages": "info",
        "duration": "neutral",
        "landing": "success",
        "recency": "warning",
        "other": "danger",
    }
    role_titles = {
        "control": "Control models",
        "treatment": "Treatment models",
        "conversion": "Conversion models",
    }

    chart_sections: list[str] = []
    for role in roles_with_data:
        series_parts: list[str] = []
        for b in BUCKETS:
            if b == "other" and all(v == 0 for v in report.bucket_shares[role][b]):
                continue
            series_parts.append(
                "{\n"
                f"                      name: {_tsx_str(bucket_labels[b])},\n"
                f"                      data: [...BUCKET_SHARES.{role}.{b}],\n"
                f"                      tone: {_tsx_str(bucket_tones[b])},\n"
                "                    }"
            )
        series_joined = ",\n                    ".join(series_parts)
        chart_sections.append(
            "\n".join(
                [
                    f'            <Card key="{role}">',
                    f"              <CardHeader>{role_titles[role]}</CardHeader>",
                    "              <CardBody>",
                    "                <BarChart",
                    "                  categories={[...SETTINGS]}",
                    "                  series={[",
                    f"                    {series_joined}",
                    "                  ]}",
                    "                  stacked",
                    "                  normalized",
                    "                  height={220}",
                    '                  valueSuffix="%"',
                    "                />",
                    '                <Text tone="secondary" size="small">',
                    "                  Share of total mean |SHAP| by feature family",
                    "                </Text>",
                    "              </CardBody>",
                    "            </Card>",
                ]
            )
        )

    control_series_js = ",\n            ".join(
        "{\n"
        f'              name: {_tsx_str(s["name"])},\n'
        f'              data: {json.dumps(s["data"])},\n'
        f'              tone: {_tsx_str(s["tone"])},\n'
        "            }"
        for s in report.control_series
    )

    findings_blocks: list[str] = []
    for i, (title, body) in enumerate(report.findings, start=1):
        findings_blocks.append(
            f"          <H3>{_jsx_text(f'{i}. {title}')}</H3>\n"
            f"          <Text>{_jsx_text(body)}</Text>"
        )
    findings_jsx = "\n\n".join(findings_blocks)

    landing_section = ""
    if report.landing_rows:
        landing_note = (
            "Landing features are shown with rank and share of total mean |SHAP| "
            "within each role. High rank on control/treatment usually means the "
            "landing signal is actionable for uplift heads."
        )
        landing_section = "\n".join(
            [
                "",
                "      <Stack gap={12}>",
                "        <H2>Landing-page signal</H2>",
                "        <Table",
                '          headers={["Setting", "Feature", "Control share", "Treatment share", "Conversion share"]}',
                f"          rows={{{_tsx_list_of_lists(report.landing_rows)}}}",
                "        />",
                '        <Callout tone="success">',
                f"          {_jsx_text(landing_note)}",
                "        </Callout>",
                "      </Stack>",
            ]
        )

    pills = [
        f'<Pill tone="info">{len(report.settings)} settings</Pill>',
        '<Pill tone="neutral">control · treatment · conversion</Pill>',
    ]
    if report.landing_rows:
        pills.append('<Pill tone="success">landing feature present</Pill>')

    callout = (
        "Auto-generated from mean |SHAP| tables. Absolute SHAP magnitudes are not "
        "comparable across LSTM vs XGB or conversion vs group models — use ranks "
        "and within-model shares."
    )

    parts = [
        'import {',
        "  BarChart,",
        "  Callout,",
        "  Card,",
        "  CardBody,",
        "  CardHeader,",
        "  Divider,",
        "  Grid,",
        "  H1,",
        "  H2,",
        "  H3,",
        "  Pill,",
        "  Row,",
        "  Stack,",
        "  Stat,",
        "  Table,",
        "  Text,",
        "  useHostTheme,",
        '} from "cursor/canvas";',
        "",
        "const SETTINGS = [",
        f"  {settings_literal},",
        "] as const;",
        "",
        bucket_block,
        "",
        "export default function ShapSummaryReport() {",
        "  const theme = useHostTheme();",
        "",
        "  return (",
        "    <Stack gap={24} style={{ padding: 24, maxWidth: 1100 }}>",
        "      <Stack gap={8}>",
        f"        <H1>{_jsx_text(report.title)}</H1>",
        '        <Text tone="secondary">',
        f"          {_jsx_text(report.source_label)}",
        "        </Text>",
        "        <Row gap={8} wrap>",
        f"          {' '.join(pills)}",
        "        </Row>",
        "      </Stack>",
        "",
        "      <Grid columns={4} gap={12}>",
        f'        <Stat value={_jsx_text(report.stats["pages_range"])} label="Page features (control)" tone="info" />',
        f'        <Stat value={_jsx_text(report.stats["best_landing"])} label="Best landing share (control)" tone="success" />',
        f'        <Stat value={_jsx_text(report.stats["weak_landing"])} label="Weakest landing share (control)" tone="warning" />',
        f'        <Stat value={_jsx_text(report.stats["degenerate"])} label="Degenerate treatment flag" tone="danger" />',
        "      </Grid>",
        "",
        '      <Callout tone="info">',
        f"        {_jsx_text(callout)}",
        "      </Callout>",
        "",
        "      <Divider />",
        "",
        "      <Stack gap={12}>",
        "        <H2>Scope</H2>",
        "        <Table",
        '          headers={["Setting", "Backend", "Landing feature", "Sample n", "Best F1"]}',
        f"          rows={{{_tsx_list_of_lists(report.setting_scope_rows)}}}",
        "        />",
        "      </Stack>",
        "",
        "      <Stack gap={12}>",
        "        <H2>Feature-family mix by role</H2>",
        '        <Text tone="secondary">',
        "          Share of total mean |SHAP| (%). Families: pages · duration · landing · recency · other.",
        "        </Text>",
        "        <Grid columns={1} gap={20}>",
        "\n".join(chart_sections),
        "        </Grid>",
        "      </Stack>",
        "",
        "      <Stack gap={12}>",
        "        <H2>What dominates each model</H2>",
        "        <Table",
        '          headers={["Setting", "Sample n", "Best F1", "Control #1", "Treatment #1", "Conversion #1"]}',
        f"          rows={{{_tsx_list_of_lists(report.top1_rows)}}}",
        "        />",
        "      </Stack>",
        "",
        "      <Stack gap={12}>",
        "        <H2>Control: mean |SHAP| by feature</H2>",
        '        <Text tone="secondary">',
        "          Absolute magnitudes are not comparable across backends. Use ranks within a series.",
        "        </Text>",
        "        <BarChart",
        "          horizontal",
        f"          categories={{{json.dumps(report.control_features)}}}",
        "          series={[",
        f"            {control_series_js}",
        "          ]}",
        "          height={320}",
        "        />",
        '        <Text tone="secondary" size="small">',
        "          Mean |SHAP| on control models",
        "        </Text>",
        "      </Stack>",
        landing_section,
        "",
        "      <Stack gap={12}>",
        "        <H2>Findings & caveats</H2>",
        "        <Stack gap={10}>",
        findings_jsx,
        "        </Stack>",
        "      </Stack>",
        "",
        "      <Card>",
        "        <CardHeader>Practical takeaway</CardHeader>",
        "        <CardBody>",
        f"          <Text>{_jsx_text(report.takeaway)}</Text>",
        "        </CardBody>",
        "      </Card>",
        "",
        '      <Text tone="secondary" size="small" style={{ color: theme.text.tertiary }}>',
        f"        {_jsx_text(' · '.join(report.footnotes))}",
        "      </Text>",
        "    </Stack>",
        "  );",
        "}",
        "",
    ]
    return "\n".join(parts)


def resolve_pdf_path(shap_results: Path, pdf_output: Path | None, canvas_output: Path | None) -> Path:
    if pdf_output is not None:
        path = pdf_output.expanduser().resolve()
        if path.suffix.lower() != ".pdf":
            path = path.with_suffix(".pdf")
        return path
    if canvas_output is not None:
        name = canvas_output.name
        if name.endswith(".canvas.tsx"):
            return canvas_output.with_name(name[: -len(".canvas.tsx")] + ".pdf")
        return canvas_output.with_suffix(".pdf")
    stem = _slugify(shap_results.name) or "shap-results"
    return (shap_results / f"{stem}-shap-summary.pdf").resolve()


def _draw_table(ax, headers: list[str], rows: list[list[str]], title: str | None = None) -> None:
    ax.axis("off")
    if title:
        ax.set_title(title, loc="left", fontsize=12, fontweight="bold", pad=8)
    if not rows:
        ax.text(0, 0.5, "(no rows)", fontsize=10, color="#6B7280")
        return
    table = ax.table(
        cellText=rows,
        colLabels=headers,
        loc="upper left",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.35)
    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor("#E5E7EB")
        if r == 0:
            cell.set_facecolor("#F3F4F6")
            cell.set_text_props(fontweight="bold")


def _wrap_cells(rows: list[list[str]], width: int = 28) -> list[list[str]]:
    wrapped: list[list[str]] = []
    for row in rows:
        wrapped.append(["\n".join(textwrap.wrap(str(c), width=width)) or str(c) for c in row])
    return wrapped


def render_pdf(report: ReportData, output: Path) -> Path:
    """Write a multi-page PDF mirroring the canvas report."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.backends.backend_pdf import PdfPages

    output.parent.mkdir(parents=True, exist_ok=True)
    settings = report.settings
    x = np.arange(len(settings))

    with PdfPages(output) as pdf:
        # Page 1 — cover + scope + stats
        fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
        fig.suptitle(report.title, fontsize=16, fontweight="bold", y=0.97)
        fig.text(0.06, 0.91, report.source_label, fontsize=8, color="#6B7280")

        ax_stats = fig.add_axes([0.06, 0.72, 0.88, 0.14])
        ax_stats.axis("off")
        labels = [
            ("Page features (control)", report.stats["pages_range"]),
            ("Best landing (control)", report.stats["best_landing"]),
            ("Weakest landing (control)", report.stats["weak_landing"]),
            ("Degenerate treatment", report.stats["degenerate"]),
        ]
        for i, (lab, val) in enumerate(labels):
            ax_stats.text(
                0.05 + i * 0.24,
                0.65,
                val,
                fontsize=14,
                fontweight="bold",
                transform=ax_stats.transAxes,
            )
            ax_stats.text(
                0.05 + i * 0.24,
                0.25,
                lab,
                fontsize=8,
                color="#6B7280",
                transform=ax_stats.transAxes,
            )

        ax_scope = fig.add_axes([0.06, 0.08, 0.88, 0.58])
        _draw_table(
            ax_scope,
            ["Setting", "Backend", "Landing feature", "Sample n", "Best F1"],
            _wrap_cells(report.setting_scope_rows, width=42),
            title="Scope",
        )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        # Page 2 — feature-family mix by role
        fig, axes = plt.subplots(3, 1, figsize=(11.69, 8.27), sharex=True)
        fig.suptitle("Feature-family mix by role (% of mean |SHAP|)", fontsize=14, fontweight="bold")
        for ax, role in zip(axes, ROLES):
            bottom = np.zeros(len(settings))
            for b in BUCKETS:
                vals = np.array(report.bucket_shares[role].get(b, [0.0] * len(settings)), dtype=float)
                if b == "other" and np.all(vals == 0):
                    continue
                ax.bar(
                    x,
                    vals,
                    bottom=bottom,
                    label=BUCKET_LABELS[b],
                    color=BUCKET_COLORS[b],
                    width=0.55,
                )
                bottom = bottom + vals
            ax.set_ylabel(role.capitalize())
            ax.set_ylim(0, 100)
            ax.set_yticks([0, 25, 50, 75, 100])
            ax.grid(axis="y", linestyle=":", alpha=0.5)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
        axes[-1].set_xticks(x)
        axes[-1].set_xticklabels(settings, rotation=15, ha="right")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper right", ncol=5, fontsize=8, frameon=False)
        fig.tight_layout(rect=[0, 0, 1, 0.95])
        pdf.savefig(fig)
        plt.close(fig)

        # Page 3 — top-1 + control bars
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.suptitle("Drivers", fontsize=14, fontweight="bold")
        ax_top = fig.add_axes([0.06, 0.58, 0.88, 0.32])
        _draw_table(
            ax_top,
            ["Setting", "Sample n", "Best F1", "Control #1", "Treatment #1", "Conversion #1"],
            _wrap_cells(report.top1_rows, width=22),
            title="What dominates each model",
        )

        ax_ctrl = fig.add_axes([0.12, 0.08, 0.8, 0.42])
        n_feat = len(report.control_features)
        n_series = max(len(report.control_series), 1)
        y = np.arange(n_feat)
        height = min(0.8 / n_series, 0.25)
        for i, series in enumerate(report.control_series):
            offset = (i - (n_series - 1) / 2) * height
            ax_ctrl.barh(
                y + offset,
                series["data"],
                height=height * 0.9,
                label=series["name"],
            )
        ax_ctrl.set_yticks(y)
        ax_ctrl.set_yticklabels(report.control_features, fontsize=8)
        ax_ctrl.invert_yaxis()
        ax_ctrl.set_xlabel("Mean |SHAP| (control)")
        ax_ctrl.set_title("Control: mean |SHAP| by feature", loc="left", fontsize=11, fontweight="bold")
        ax_ctrl.legend(fontsize=7, loc="lower right", frameon=False)
        ax_ctrl.spines["top"].set_visible(False)
        ax_ctrl.spines["right"].set_visible(False)
        pdf.savefig(fig)
        plt.close(fig)

        # Page 4 — landing + findings
        fig = plt.figure(figsize=(11.69, 8.27))
        fig.suptitle("Landing signal & findings", fontsize=14, fontweight="bold")
        ax_land = fig.add_axes([0.06, 0.62, 0.88, 0.28])
        if report.landing_rows:
            _draw_table(
                ax_land,
                ["Setting", "Feature", "Control", "Treatment", "Conversion"],
                _wrap_cells(report.landing_rows, width=22),
                title="Landing-page signal",
            )
        else:
            ax_land.axis("off")
            ax_land.set_title("Landing-page signal", loc="left", fontsize=12, fontweight="bold")
            ax_land.text(0, 0.5, "No landing feature in this run.", fontsize=10, color="#6B7280")

        ax_find = fig.add_axes([0.06, 0.08, 0.88, 0.48])
        ax_find.axis("off")
        ax_find.set_title("Findings & caveats", loc="left", fontsize=12, fontweight="bold", pad=8)
        y_pos = 0.92
        for i, (title, body) in enumerate(report.findings, start=1):
            wrapped_title = textwrap.fill(f"{i}. {title}", width=110)
            wrapped_body = textwrap.fill(body, width=110)
            ax_find.text(0, y_pos, wrapped_title, fontsize=9, fontweight="bold", va="top")
            y_pos -= 0.04 + 0.015 * wrapped_title.count("\n")
            ax_find.text(0, y_pos, wrapped_body, fontsize=8, color="#374151", va="top")
            y_pos -= 0.06 + 0.018 * wrapped_body.count("\n")
            if y_pos < 0.08:
                break
        pdf.savefig(fig)
        plt.close(fig)

        # Page 5 — takeaway
        fig = plt.figure(figsize=(11.69, 8.27))
        ax = fig.add_axes([0.1, 0.2, 0.8, 0.6])
        ax.axis("off")
        ax.set_title("Practical takeaway", loc="left", fontsize=14, fontweight="bold", pad=12)
        ax.text(
            0,
            0.7,
            textwrap.fill(report.takeaway, width=100),
            fontsize=11,
            va="top",
            color="#111827",
        )
        ax.text(
            0,
            0.15,
            textwrap.fill(" · ".join(report.footnotes), width=110),
            fontsize=8,
            color="#6B7280",
            va="top",
        )
        pdf.savefig(fig)
        plt.close(fig)

    return output


def _html_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _html_table(headers: list[str], rows: list[list[str]]) -> str:
    th = "".join(f"<th>{_html_escape(h)}</th>" for h in headers)
    body_rows = []
    for row in rows:
        tds = "".join(f"<td>{_html_escape(c)}</td>" for c in row)
        body_rows.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def resolve_html_path(
    shap_results: Path, html_output: Path | None, canvas_output: Path | None
) -> Path:
    if html_output is not None:
        path = html_output.expanduser().resolve()
        if path.suffix.lower() != ".html":
            path = path.with_suffix(".html")
        return path
    if canvas_output is not None:
        name = canvas_output.name
        if name.endswith(".canvas.tsx"):
            return canvas_output.with_name(name[: -len(".canvas.tsx")] + ".html")
        return canvas_output.with_suffix(".html")
    stem = _slugify(shap_results.name) or "shap-results"
    return (shap_results / f"{stem}-shap-summary.html").resolve()


def render_html(report: ReportData, output: Path) -> Path:
    """Write a standalone HTML report (Chart.js CDN) mirroring the canvas."""
    output.parent.mkdir(parents=True, exist_ok=True)

    active_buckets = [
        b
        for b in BUCKETS
        if b != "other"
        or any(
            any(v != 0 for v in report.bucket_shares[role].get(b, [])) for role in ROLES
        )
    ]

    def stacked_dataset(role: str) -> str:
        parts = []
        for b in active_buckets:
            vals = report.bucket_shares[role].get(b, [0.0] * len(report.settings))
            parts.append(
                "{"
                f'label:{json.dumps(BUCKET_LABELS[b])},'
                f"data:{json.dumps(vals)},"
                f'backgroundColor:{json.dumps(BUCKET_COLORS[b])},'
                "stack:'share'"
                "}"
            )
        return "[" + ",".join(parts) + "]"

    control_datasets = []
    palette = ["#3B82F6", "#6B7280", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6"]
    for i, series in enumerate(report.control_series):
        control_datasets.append(
            "{"
            f'label:{json.dumps(series["name"])},'
            f'data:{json.dumps(series["data"])},'
            f"backgroundColor:{json.dumps(palette[i % len(palette)])}"
            "}"
        )

    findings_html = "".join(
        f"<div class='finding'><h3>{_html_escape(f'{i}. {title}')}</h3>"
        f"<p>{_html_escape(body)}</p></div>"
        for i, (title, body) in enumerate(report.findings, start=1)
    )

    landing_section = ""
    if report.landing_rows:
        landing_section = f"""
    <section>
      <h2>Landing-page signal</h2>
      {_html_table(
          ["Setting", "Feature", "Control share", "Treatment share", "Conversion share"],
          report.landing_rows,
      )}
    </section>"""

    chart_blocks = []
    for role in ROLES:
        chart_blocks.append(
            f"""
      <div class="card">
        <h3>{role.capitalize()} models</h3>
        <canvas id="chart-{role}" height="120"></canvas>
      </div>"""
        )

    chart_inits = []
    for role in ROLES:
        chart_inits.append(
            f"""
  new Chart(document.getElementById('chart-{role}'), {{
    type: 'bar',
    data: {{
      labels: {json.dumps(report.settings)},
      datasets: {stacked_dataset(role)}
    }},
    options: {{
      responsive: true,
      scales: {{
        x: {{ stacked: true }},
        y: {{ stacked: true, max: 100, title: {{ display: true, text: '% of mean |SHAP|' }} }}
      }}
    }}
  }});"""
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{_html_escape(report.title)}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0b0d10;
      --panel: #14181f;
      --text: #e8eaed;
      --muted: #9aa3af;
      --border: #2a3140;
      --accent: #3b82f6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.45;
    }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 32px 20px 64px; }}
    h1 {{ font-size: 1.75rem; margin: 0 0 8px; }}
    h2 {{ font-size: 1.25rem; margin: 32px 0 12px; }}
    h3 {{ font-size: 1rem; margin: 0 0 12px; }}
    .muted {{ color: var(--muted); font-size: 0.9rem; }}
    .pills {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0 20px; }}
    .pill {{
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 0.8rem;
      color: var(--muted);
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 16px 0 24px;
    }}
    .stat, .card, .callout, .finding {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px 16px;
    }}
    .stat .value {{ font-size: 1.35rem; font-weight: 700; }}
    .stat .label {{ color: var(--muted); font-size: 0.8rem; margin-top: 4px; }}
    .callout {{ margin: 12px 0 24px; color: var(--muted); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
      overflow: auto;
      display: block;
    }}
    th, td {{
      border-bottom: 1px solid var(--border);
      text-align: left;
      padding: 8px 10px;
      vertical-align: top;
      white-space: pre-wrap;
    }}
    th {{ color: var(--muted); font-weight: 600; }}
    .grid {{ display: grid; gap: 16px; }}
    .finding {{ margin-bottom: 10px; }}
    .finding p {{ margin: 6px 0 0; color: var(--muted); }}
    footer {{ margin-top: 32px; color: var(--muted); font-size: 0.75rem; }}
    @media (max-width: 800px) {{
      .stats {{ grid-template-columns: 1fr 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <h1>{_html_escape(report.title)}</h1>
    <p class="muted">{_html_escape(report.source_label)}</p>
    <div class="pills">
      <span class="pill">{len(report.settings)} settings</span>
      <span class="pill">control · treatment · conversion</span>
      {"<span class='pill'>landing feature present</span>" if report.landing_rows else ""}
    </div>

    <div class="stats">
      <div class="stat"><div class="value">{_html_escape(report.stats["pages_range"])}</div><div class="label">Page features (control)</div></div>
      <div class="stat"><div class="value">{_html_escape(report.stats["best_landing"])}</div><div class="label">Best landing share (control)</div></div>
      <div class="stat"><div class="value">{_html_escape(report.stats["weak_landing"])}</div><div class="label">Weakest landing share (control)</div></div>
      <div class="stat"><div class="value">{_html_escape(report.stats["degenerate"])}</div><div class="label">Degenerate treatment flag</div></div>
    </div>

    <div class="callout">
      Absolute SHAP magnitudes are not comparable across LSTM vs XGB or conversion vs group models — use ranks and within-model shares.
      Note: LSTM sample n of 200 usually means KernelExplainer fallback after GradientExplainer failed; XGB may use the full dataset.
    </div>

    <section>
      <h2>Scope</h2>
      {_html_table(["Setting", "Backend", "Landing feature", "Sample n", "Best F1"], report.setting_scope_rows)}
    </section>

    <section>
      <h2>Feature-family mix by role</h2>
      <p class="muted">Share of total mean |SHAP| (%). Families: pages · duration · landing · recency · other.</p>
      <div class="grid">
        {"".join(chart_blocks)}
      </div>
    </section>

    <section>
      <h2>What dominates each model</h2>
      {_html_table(["Setting", "Sample n", "Best F1", "Control #1", "Treatment #1", "Conversion #1"], report.top1_rows)}
    </section>

    <section>
      <h2>Control: mean |SHAP| by feature</h2>
      <p class="muted">Absolute magnitudes are not comparable across backends. Use ranks within a series.</p>
      <div class="card">
        <canvas id="chart-control-features" height="160"></canvas>
      </div>
    </section>
    {landing_section}

    <section>
      <h2>Findings &amp; caveats</h2>
      {findings_html}
    </section>

    <section>
      <h2>Practical takeaway</h2>
      <div class="card"><p>{_html_escape(report.takeaway)}</p></div>
    </section>

    <footer>{_html_escape(" · ".join(report.footnotes))}</footer>
  </main>

  <script>
{"".join(chart_inits)}

  new Chart(document.getElementById('chart-control-features'), {{
    type: 'bar',
    data: {{
      labels: {json.dumps(report.control_features)},
      datasets: [{",".join(control_datasets)}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      scales: {{
        x: {{ title: {{ display: true, text: 'Mean |SHAP|' }} }}
      }}
    }}
  }});
  </script>
</body>
</html>
"""
    output.write_text(html, encoding="utf-8")
    return output


def main() -> None:
    args = parse_args()
    shap_results = args.shap_results.expanduser().resolve()
    df = load_shap_frame(shap_results, cv=args.cv)
    manifest = load_manifest(shap_results)
    report = build_report(df, shap_results, title=args.title, manifest=manifest)

    skip_canvas = args.pdf_only or args.html_only
    canvas_output: Path | None = None
    if not skip_canvas:
        canvas_output = resolve_output_path(shap_results, args.output)
        canvas_output.parent.mkdir(parents=True, exist_ok=True)
        canvas_output.write_text(render_canvas(report), encoding="utf-8")
        print(f"Wrote canvas report: {canvas_output}")

    if args.pdf or args.pdf_only or args.pdf_output is not None:
        pdf_path = resolve_pdf_path(shap_results, args.pdf_output, canvas_output)
        render_pdf(report, pdf_path)
        print(f"Wrote PDF report: {pdf_path}")

    if args.html or args.html_only or args.html_output is not None:
        html_path = resolve_html_path(shap_results, args.html_output, canvas_output)
        render_html(report, html_path)
        print(f"Wrote HTML report: {html_path}")

    print(f"Settings: {', '.join(report.settings)}")
    print(f"Source: {report.source_label}")


if __name__ == "__main__":
    main()
