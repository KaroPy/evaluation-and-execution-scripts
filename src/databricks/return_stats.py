import matplotlib.pyplot as plt
import seaborn as sns
from src.databricks.data_transformation import add_columns

JOB_NAMES = {
    "write-datashifts-to-table": "write-datashifts-to-table",
    "log_active_prefect_runs": "log_active_prefect_runs",
    "save_incidents": "save_incidents",
    "etl-flow": "etl-flow",
    "conversion_prob_update_views": "conversion_prob_update_views",
    "conversion_prob_update_table": "conversion_prob_update_table",
    "attribution": "attribution",
    "views": "views",
    "etl-flow": "etl-flow",
    "monitoring_update": "monitoring_update",
    "weekly_report": "weekly_report",
}


def count_failed_and_success(df, path, logger):
    df = add_columns(df)
    df["cleanded_run_name"] = None
    for key in JOB_NAMES.keys():
        for irow, row in df.iterrows():
            if key in row["run_name"]:
                df.at[irow, "cleanded_run_name"] = JOB_NAMES[key]
    vc_not_specified = df[df["cleanded_run_name"].isnull()]
    if not vc_not_specified.empty:
        raise ValueError(
            f"Found run names not specified in JOB_NAMES: {vc_not_specified['run_name'].unique()}"
        )
    count_states_by_name = (
        df.groupby("cleanded_run_name")["result_state"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_states_by_name["success_rate"] = (
        count_states_by_name.get("SUCCESS", 0)
        / (
            count_states_by_name.get("SUCCESS", 0)
            + count_states_by_name.get("FAILED", 0)
        )
        * 100
    )
    logger.info("Job run success and failure counts:")
    logger.info(count_states_by_name)
    count_states_by_name.to_csv(f"{path}_count_states_by_name.csv")

    count_states_by_name_and_week = (
        df.groupby(by=["cleanded_run_name", "year-week"])["result_state"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_states_by_name_and_week["count_failed_and_completed"] = (
        count_states_by_name_and_week.get("SUCCESS", 0)
        + count_states_by_name_and_week.get("FAILED", 0)
    )
    count_states_by_name_and_week["success_rate"] = (
        count_states_by_name_and_week.get("SUCCESS", 0)
        / count_states_by_name_and_week.get("count_failed_and_completed", 0)
        * 100
    )
    count_states_by_name_and_week = count_states_by_name_and_week.reset_index()
    count_states_by_name_and_week = count_states_by_name_and_week.sort_values(
        by=["cleanded_run_name", "year-week"]
    )
    logger.info("Job run success and failure counts by week:")
    logger.info(count_states_by_name_and_week)
    count_states_by_name_and_week.to_csv(f"{path}_count_states_by_name_and_week.csv")
    plot_counts_databricks(
        count_states_by_name_and_week,
        path,
        logger,
        x_value="year-week",
        y_value="success_rate",
    )

    count_states_by_name_and_month = (
        df.groupby(by=["cleanded_run_name", "year-month"])["result_state"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_states_by_name_and_month["count_failed_and_completed"] = (
        count_states_by_name_and_month.get("SUCCESS", 0)
        + count_states_by_name_and_month.get("FAILED", 0)
    )
    count_states_by_name_and_month["success_rate"] = (
        count_states_by_name_and_month.get("SUCCESS", 0)
        / count_states_by_name_and_month.get("count_failed_and_completed", 0)
        * 100
    )
    count_states_by_name_and_month = count_states_by_name_and_month.reset_index()
    count_states_by_name_and_month = count_states_by_name_and_month.sort_values(
        by=["cleanded_run_name", "year-month"]
    )
    logger.info("Job run success and failure counts by month:")
    logger.info(count_states_by_name_and_month)
    count_states_by_name_and_month.to_csv(f"{path}_count_states_by_name_and_month.csv")
    plot_counts_databricks(
        count_states_by_name_and_month,
        path,
        logger,
        x_value="year-month",
        y_value="success_rate",
    )


def plot_counts_databricks(df, path, logger, x_value="date", y_value="success_rate"):
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    sns.barplot(data=df, x=x_value, y=y_value, hue="cleanded_run_name")
    ax.set_title(f"{y_value} over time")
    ax.set_xlabel(x_value.capitalize())
    ax.set_ylabel(y_value)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"{path}_{y_value}_over_time_{x_value}.png"
    plt.savefig(plot_path)

    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    sns.barplot(data=df, x=x_value, y=y_value, hue="cleanded_run_name")
    ax.set_title(f"{y_value} over time")
    ax.set_xlabel(x_value.capitalize())
    ax.set_ylabel(y_value)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"{path}_{y_value}_over_time_{x_value}_line.png"
    plt.savefig(plot_path)
