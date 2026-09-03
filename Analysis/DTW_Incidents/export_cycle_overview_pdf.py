"""
Generate a cycle-planning PDF overview from signal- and treatment-based DTW analyses.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages

from Analysis.DTW_Incidents.dtw_functions import combine_incidents, merge_dtw_daily_with_pylon
from general_functions.return_workspace_ids import return_workspace_ids

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "Analysis" / "DTW_Incidents"
OUT_PDF = OUT_DIR / "2026-07-20-dtw-incident-cycle-overview.pdf"

SIGNAL_CSV = OUT_DIR / "2026-07-20-dtw_incident_daily-3.csv"
TREATMENT_CSV = OUT_DIR / "2026-07-20-dtw_incident_daily_v2_treatment_based.csv"
PYLON_CSV = OUT_DIR / "2026-07-20_pylon_tickets_dtw_only.csv"

COLORS = {
    "conversion": "#2F6FED",
    "causal": "#E67E22",
    "signal": "#2F6FED",
    "treatment": "#0B8A5B",
    "accent": "#C0392B",
}


def _style():
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.titleweight": "bold",
            "axes.labelweight": "medium",
            "font.size": 11,
        }
    )


def load_signal(workspace_ids: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(SIGNAL_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df["is_incident"] = df["is_incident"].astype(bool)
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["year"] = df["date"].dt.isocalendar().year.astype(int)
    df["month"] = df["date"].dt.month.astype(int)
    df["year-week"] = df["year"].astype(str) + "-" + df["week"].astype(str).str.zfill(2)
    df["year-month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    df["workspace_id"] = df["account_name"].map(workspace_ids)
    df["grain"] = "signal"
    return df


def load_treatment(workspace_ids: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(TREATMENT_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df["is_incident"] = df["is_incident"].astype(bool)
    df["meets_pylon_spend_threshold"] = df["meets_pylon_spend_threshold"].astype(bool)
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["year"] = df["date"].dt.isocalendar().year.astype(int)
    df["month"] = df["date"].dt.month.astype(int)
    df["year-week"] = df["year"].astype(str) + "-" + df["week"].astype(str).str.zfill(2)
    df["year-month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    df["workspace_id"] = df["account_name"].map(workspace_ids)
    df["grain"] = "treatment"
    return df


def weekly_rates(df: pd.DataFrame) -> pd.DataFrame:
    w = (
        df.groupby(["account_name", "year", "week", "model_type", "year-week"], as_index=False)
        .agg(incident_days=("is_incident", "sum"), total_days=("is_incident", "count"))
    )
    w["is_incident"] = w["incident_days"] / w["total_days"]
    return w.sort_values(["year", "week"])


def monthly_rates(df: pd.DataFrame) -> pd.DataFrame:
    m = (
        df.groupby(["account_name", "year", "month", "model_type", "year-month"], as_index=False)
        .agg(incident_days=("is_incident", "sum"), total_days=("is_incident", "count"))
    )
    m["is_incident"] = m["incident_days"] / m["total_days"]
    return m.sort_values(["year", "month"])


def page_title(pdf: PdfPages):
    fig = plt.figure(figsize=(11.69, 8.27))  # A4 landscape
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    ax.text(0.06, 0.72, "DTW Incident Analysis", fontsize=28, fontweight="bold", color="#1A1A1A")
    ax.text(0.06, 0.62, "Cycle planning overview · May–July 2026", fontsize=16, color="#555555")
    ax.text(
        0.06,
        0.48,
        "Signal-based vs treatment-based daily DTW flags\n"
        "merged with Pylon tickets · episode durations · tag mix",
        fontsize=13,
        color="#333333",
        linespacing=1.5,
    )
    ax.add_patch(plt.Rectangle((0.06, 0.38), 0.25, 0.012, color=COLORS["treatment"], transform=ax.transAxes))
    ax.text(
        0.06,
        0.22,
        "Takeaway: use treatment grain for planning KPIs.\n"
        "Signal grain overstates firefighting load; most DTW days never get a Pylon ticket.",
        fontsize=12,
        color="#1A1A1A",
        linespacing=1.6,
        fontweight="medium",
    )
    ax.text(0.06, 0.08, "Generated from Analysis/DTW_Incidents notebooks", fontsize=9, color="#888888")
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def page_scope(pdf: PdfPages, signal: pd.DataFrame, treatment: pd.DataFrame, pylon_stats: dict):
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("Scope comparison", fontsize=18, fontweight="bold", y=0.98)

    rows = pd.DataFrame(
        {
            "Metric": [
                "Rows",
                "Entities",
                "Accounts",
                "Daily incident rate",
                "Conversion share",
                "Pylon tickets matched",
                "DTW rows with ticket",
            ],
            "Signal": [
                f"{len(signal):,}",
                f"{signal['audience_id'].nunique()} signals",
                str(signal["account_name"].nunique()),
                f"{signal['is_incident'].mean():.0%}",
                f"{(signal['model_type']=='conversion').mean():.0%}",
                f"{pylon_stats['signal_tickets_matched']}/{pylon_stats['pylon_total']}",
                f"{pylon_stats['signal_rows_matched']:,}",
            ],
            "Treatment": [
                f"{len(treatment):,}",
                f"{treatment['treatment_id'].nunique()} treatments",
                str(treatment["account_name"].nunique()),
                f"{treatment['is_incident'].mean():.0%}",
                f"{(treatment['model_type']=='conversion').mean():.0%}",
                f"{pylon_stats['treatment_tickets_matched']}/{pylon_stats['pylon_total']}",
                f"{pylon_stats['treatment_rows_matched']:,}",
            ],
        }
    )

    axes[0].axis("off")
    table = axes[0].table(
        cellText=rows.values,
        colLabels=rows.columns,
        loc="center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)
    for (r, c), cell in table.get_celld().items():
        if r == 0:
            cell.set_facecolor("#1A1A1A")
            cell.set_text_props(color="white", fontweight="bold")
        elif r % 2 == 0:
            cell.set_facecolor("#F4F6F8")
        cell.set_edgecolor("#DDDDDD")
    axes[0].set_title("Dataset footprint", pad=12)

    compare = pd.DataFrame(
        {
            "grain": ["Signal", "Treatment"],
            "incident_rate": [signal["is_incident"].mean(), treatment["is_incident"].mean()],
        }
    )
    axes[1].bar(
        compare["grain"],
        compare["incident_rate"],
        color=[COLORS["signal"], COLORS["treatment"]],
        width=0.55,
    )
    axes[1].set_ylim(0, 1)
    axes[1].set_ylabel("Daily incident rate")
    axes[1].set_xlabel("")
    axes[1].set_title("Incident intensity by grain")
    for i, v in enumerate(compare["incident_rate"]):
        axes[1].text(i, v + 0.03, f"{v:.0%}", ha="center", fontweight="bold")

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


def page_weekly_trends(pdf: PdfPages, signal_w: pd.DataFrame, treatment_w: pd.DataFrame):
    fig, axes = plt.subplots(2, 1, figsize=(11.69, 8.27), sharex=False)
    fig.suptitle("Weekly conversion incident rate (account mean ± spread)", fontsize=16, fontweight="bold")

    for ax, data, title, color in [
        (axes[0], signal_w, "Signal grain", COLORS["signal"]),
        (axes[1], treatment_w, "Treatment grain", COLORS["treatment"]),
    ]:
        conv = data[data["model_type"] == "conversion"]
        agg = conv.groupby("year-week")["is_incident"].agg(
            mean="mean",
            q25=lambda s: s.quantile(0.25),
            q75=lambda s: s.quantile(0.75),
        ).reset_index()
        # Force plain float arrays (avoid object-dtype from named agg edge cases)
        weeks = [str(w) for w in agg["year-week"].tolist()]
        y_mean = [float(v) for v in agg["mean"].tolist()]
        y_q25 = [float(v) for v in agg["q25"].tolist()]
        y_q75 = [float(v) for v in agg["q75"].tolist()]
        x = list(range(len(weeks)))
        ax.fill_between(x, y_q25, y_q75, color=color, alpha=0.2)
        ax.plot(x, y_mean, marker="o", color=color, linewidth=2)
        ax.set_xticks(x)
        ax.set_xticklabels(weeks, rotation=90)
        ax.set_ylim(0, 1.05)
        ax.set_ylabel("Incident rate")
        ax.set_title(title)

    fig.tight_layout(rect=[0, 0.02, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


def page_monthly_and_accounts(pdf: PdfPages, treatment: pd.DataFrame, treatment_w: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("Treatment-based: monthly rates & account spread", fontsize=16, fontweight="bold")

    monthly = monthly_rates(treatment)
    sns.barplot(
        data=monthly,
        x="year-month",
        y="is_incident",
        hue="model_type",
        palette={"conversion": COLORS["conversion"], "causal": COLORS["causal"]},
        errorbar="sd",
        ax=axes[0],
    )
    axes[0].set_ylim(0, 1.05)
    axes[0].set_title("Monthly incident rate")
    axes[0].set_ylabel("Incident rate")
    axes[0].tick_params(axis="x", rotation=0)

    sns.boxplot(
        data=treatment_w,
        x="account_name",
        y="is_incident",
        hue="model_type",
        palette={"conversion": COLORS["conversion"], "causal": COLORS["causal"]},
        ax=axes[1],
    )
    axes[1].set_ylim(-0.05, 1.05)
    axes[1].set_title("Weekly rates by account")
    axes[1].tick_params(axis="x", rotation=90)
    axes[1].set_xlabel("")
    axes[1].legend(title="Model", loc="upper right", fontsize=8)

    fig.tight_layout(rect=[0, 0.02, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


def page_episodes(pdf: PdfPages, signal_eps: pd.DataFrame, treatment_eps: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("Incident episode durations (3-day end rule)", fontsize=16, fontweight="bold")

    for ax, eps, title in [
        (axes[0], signal_eps, "Signal grain"),
        (axes[1], treatment_eps, "Treatment grain"),
    ]:
        inc = eps[eps["period_type"] == "incident"].copy()
        inc["label"] = inc["model_type"].astype(str)
        sns.histplot(
            data=inc,
            x="duration_days",
            hue="label",
            element="step",
            stat="density",
            common_norm=False,
            palette={"conversion": COLORS["conversion"], "causal": COLORS["causal"]},
            ax=ax,
        )
        ax.set_title(title)
        ax.set_xlabel("Duration (days)")
        med = inc.groupby("model_type")["duration_days"].median()
        txt = "  |  ".join([f"{k}: median {v:.0f}d" for k, v in med.items()])
        ax.text(0.98, 0.95, txt, transform=ax.transAxes, ha="right", va="top", fontsize=9, color="#333333")

    fig.tight_layout(rect=[0, 0.02, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


def page_tags_and_spend(pdf: PdfPages, treatment_pylon: pd.DataFrame, treatment: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
    fig.suptitle("Pylon labels & spend threshold (treatment view)", fontsize=16, fontweight="bold")

    tags = treatment_pylon["tags"].fillna("no_pylon_ticket").copy()
    # explode multi-tags for clearer counts among labeled tickets
    labeled = treatment_pylon[treatment_pylon["issue_id"].notna()].copy()
    if labeled.empty:
        axes[0].text(0.5, 0.5, "No matched tickets", ha="center")
    else:
        exploded = (
            labeled["tags"]
            .fillna("no_tags")
            .astype(str)
            .str.split("|")
            .explode()
            .str.strip()
        )
        vc = exploded.value_counts().head(10)
        sns.barplot(x=vc.values, y=vc.index, color=COLORS["accent"], ax=axes[0])
        axes[0].set_title("Top tags on matched Pylon tickets")
        axes[0].set_xlabel("Ticket-days (exploded tags)")

    spend = (
        treatment.groupby(["meets_pylon_spend_threshold", "is_incident"], as_index=False)
        .size()
        .rename(columns={"size": "rows"})
    )
    spend["threshold"] = spend["meets_pylon_spend_threshold"].map({True: "Meets threshold", False: "Below threshold"})
    spend["incident"] = spend["is_incident"].map({True: "Incident", False: "No incident"})
    pivot = spend.pivot_table(index="threshold", columns="incident", values="rows", fill_value=0)
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0)
    pivot_pct.plot(kind="bar", stacked=True, color=["#A8D5A2", COLORS["accent"]], ax=axes[1], rot=0)
    axes[1].set_title("Incident share by spend threshold")
    axes[1].set_ylabel("Share of days")
    axes[1].set_xlabel("")
    axes[1].set_ylim(0, 1)
    axes[1].legend(title="", loc="upper right")

    # annotate rates
    for i, thr in enumerate(pivot_pct.index):
        rate = pivot_pct.loc[thr].get("Incident", 0)
        axes[1].text(i, rate + 0.03, f"{rate:.0%} incidents", ha="center", fontsize=10, fontweight="bold")

    fig.tight_layout(rect=[0, 0.02, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


def page_recommendations(pdf: PdfPages, signal_eps: pd.DataFrame, treatment_eps: pd.DataFrame, treatment: pd.DataFrame):
    fig = plt.figure(figsize=(11.69, 8.27))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")

    tr_inc = treatment_eps[(treatment_eps["period_type"] == "incident") & (treatment_eps["model_type"] == "conversion")]
    sig_inc = signal_eps[(signal_eps["period_type"] == "incident") & (signal_eps["model_type"] == "conversion")]

    bullets = [
        f"Baseline KPI (treatment / conversion): weekly incident rate ≈ {treatment[treatment.model_type=='conversion']['is_incident'].mean():.0%}; "
        f"median episode ≈ {tr_inc['duration_days'].median():.0f} days (signal median ≈ {sig_inc['duration_days'].median():.0f}).",
        "Plan capacity on treatment grain — signal grain (~72% daily) overstates load.",
        "Most DTW days have no Pylon ticket → ticket volume ≠ DTW coverage; improve gating before adding alert volume.",
        "Tag mix is dominated by Nothing / No Performance Access / Deactivated / low spend → prioritize suppressions.",
        "Causal is rare but long-lived (median weeks) — separate queue from high-throughput conversion.",
        "Spend threshold helps a bit (≈13% vs ≈50% incident rate) but does not solve the bulk problem.",
    ]

    ax.text(0.06, 0.9, "Recommendations for this cycle", fontsize=20, fontweight="bold")
    y = 0.78
    for i, b in enumerate(bullets, 1):
        ax.text(0.06, y, f"{i}.", fontsize=13, fontweight="bold", color=COLORS["treatment"])
        ax.text(0.10, y, b, fontsize=12, color="#222222", wrap=True, va="top")
        y -= 0.11

    ax.text(
        0.06,
        0.08,
        "Artifacts: 2026-07-20-incidend-analysis.ipynb · 2026-07-20-incident-analysis-treatment-based.ipynb",
        fontsize=9,
        color="#888888",
    )
    pdf.savefig(fig)
    plt.close(fig)


def main():
    _style()
    workspace_ids = {ws["name"]: ws["id"] for ws in return_workspace_ids()}
    pylon = pd.read_csv(PYLON_CSV)

    signal = load_signal(workspace_ids)
    treatment = load_treatment(workspace_ids)

    signal_pylon, signal_unmatched = merge_dtw_daily_with_pylon(signal, pylon, match_on="signal")
    treatment_pylon, treatment_unmatched = merge_dtw_daily_with_pylon(treatment, pylon, match_on="treatment")

    # normalize tags for plotting
    for df in (signal_pylon, treatment_pylon):
        df["tags"] = np.where(df["tags"].isna() & df["issue_id"].notna(), "no_tags", df["tags"])
        df["tags"] = np.where(df["tags"].isna() & df["issue_id"].isna(), "no_pylon_ticket", df["tags"])

    signal_eps = combine_incidents(signal_pylon, entity_id_col="audience_id")
    treatment_eps = combine_incidents(treatment_pylon, entity_id_col="treatment_id")

    pylon_stats = {
        "pylon_total": len(pylon),
        "signal_tickets_matched": len(pylon) - len(signal_unmatched),
        "treatment_tickets_matched": len(pylon) - len(treatment_unmatched),
        "signal_rows_matched": int(signal_pylon["issue_id"].notna().sum()),
        "treatment_rows_matched": int(treatment_pylon["issue_id"].notna().sum()),
    }

    signal_w = weekly_rates(signal)
    treatment_w = weekly_rates(treatment)

    with PdfPages(OUT_PDF) as pdf:
        page_title(pdf)
        page_scope(pdf, signal, treatment, pylon_stats)
        page_weekly_trends(pdf, signal_w, treatment_w)
        page_monthly_and_accounts(pdf, treatment, treatment_w)
        page_episodes(pdf, signal_eps, treatment_eps)
        page_tags_and_spend(pdf, treatment_pylon, treatment)
        page_recommendations(pdf, signal_eps, treatment_eps, treatment)

        meta = pdf.infodict()
        meta["Title"] = "DTW Incident Analysis — Cycle Planning Overview"
        meta["Author"] = "Innkeepr DTW analysis"
        meta["Subject"] = "May–July 2026 signal vs treatment comparison"

    print(f"Wrote {OUT_PDF}")


if __name__ == "__main__":
    main()
