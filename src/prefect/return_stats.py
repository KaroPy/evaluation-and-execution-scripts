import matplotlib.pyplot as plt
import seaborn as sns


def count_failed_and_success(df, path, logger):
    count_states_by_name = (
        df.groupby("flow_name")["state_type"].value_counts().unstack(fill_value=0)
    )
    count_states_by_name["success_rate"] = (
        count_states_by_name.get("COMPLETED", 0)
        / (
            count_states_by_name.get("COMPLETED", 0)
            + count_states_by_name.get("FAILED", 0)
        )
        * 100
    )
    logger.info("Flow run success and failure counts:")
    logger.info(count_states_by_name)
    count_states_by_name.to_csv(f"{path}_count_states_by_name.csv")

    count_states_by_name_and_date = (
        df.groupby(by=["flow_name", "date"])["state_type"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_states_by_name_and_date["count_failed_and_completed"] = (
        count_states_by_name_and_date.get("COMPLETED", 0)
        + count_states_by_name_and_date.get("FAILED", 0)
    )
    count_states_by_name_and_date["success_rate"] = (
        count_states_by_name_and_date.get("COMPLETED", 0)
        / count_states_by_name_and_date.get("count_failed_and_completed", 0)
        * 100
    )
    count_states_by_name_and_date = count_states_by_name_and_date.reset_index()
    count_states_by_name_and_date = count_states_by_name_and_date.sort_values(
        by=["flow_name", "date"]
    )
    logger.info("Flow run success and failure counts by date:")
    logger.info(count_states_by_name_and_date)
    count_states_by_name_and_date.to_csv(f"{path}_count_states_by_name_and_date.csv")
    plot_counts(count_states_by_name_and_date, path, logger)

    count_states_by_name_and_week = (
        df.groupby(by=["flow_name", "week"])["state_type"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_states_by_name_and_week["count_failed_and_completed"] = (
        count_states_by_name_and_week.get("COMPLETED", 0)
        + count_states_by_name_and_week.get("FAILED", 0)
    )
    count_states_by_name_and_week["success_rate"] = (
        count_states_by_name_and_week.get("COMPLETED", 0)
        / count_states_by_name_and_week.get("count_failed_and_completed", 0)
        * 100
    )
    count_states_by_name_and_week = count_states_by_name_and_week.reset_index()
    count_states_by_name_and_week = count_states_by_name_and_week.sort_values(
        by=["flow_name", "week"]
    )
    logger.info("Flow run success and failure counts by week:")
    logger.info(count_states_by_name_and_week)
    count_states_by_name_and_week.to_csv(f"{path}_count_states_by_name_and_week.csv")

    count_states_by_name_and_month = (
        df.groupby(by=["flow_name", "year-month"])["state_type"]
        .value_counts()
        .unstack(fill_value=0)
    )
    count_states_by_name_and_month["count_failed_and_completed"] = (
        count_states_by_name_and_month.get("COMPLETED", 0)
        + count_states_by_name_and_month.get("FAILED", 0)
    )
    count_states_by_name_and_month["success_rate"] = (
        count_states_by_name_and_month.get("COMPLETED", 0)
        / count_states_by_name_and_month.get("count_failed_and_completed", 0)
        * 100
    )
    count_states_by_name_and_month = count_states_by_name_and_month.reset_index()
    count_states_by_name_and_month = count_states_by_name_and_month.sort_values(
        by=["flow_name", "year-month"]
    )
    logger.info("Flow run success and failure counts by month:")
    logger.info(count_states_by_name_and_month)
    count_states_by_name_and_month.to_csv(f"{path}_count_states_by_name_and_month.csv")
    plot_counts(count_states_by_name_and_month, path, logger, x_value="year-month")


def plot_counts(counts_df, path, logger, x_value="date"):
    if x_value == "date":
        counts_df["date"] = counts_df["date"].astype(str)
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    sns.barplot(
        data=counts_df,
        x=x_value,
        y="success_rate",
        hue="flow_name",
        # marker="o",
        ax=ax,
    )
    ax.set_title("Flow Run Success Rate Over Time")
    ax.set_xlabel(x_value.capitalize())
    ax.set_ylabel("Success Rate (%)")
    ax.set_ylim(0, 100)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"{path}_success_rate_over_time_{x_value}.png"
    plt.savefig(plot_path)

    # line plot
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    sns.lineplot(
        data=counts_df,
        x=x_value,
        y="success_rate",
        hue="flow_name",
        marker="o",
        ax=ax,
    )
    ax.set_title("Flow Run Success Rate Over Time")
    ax.set_xlabel(x_value.capitalize())
    ax.set_ylabel("Success Rate (%)")
    ax.set_ylim(0, 100)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"{path}_success_rate_over_time_line_{x_value}_line.png"
    plt.savefig(plot_path)
    logger.info(f"Success rate plot saved to {plot_path}")


def plot_error_counts_subtasks(
    counts_df, path, logger, x_value="date", y_value="count", flow_name=None
):
    if x_value == "date":
        counts_df["date"] = counts_df["date"].astype(str)
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    sns.barplot(
        data=counts_df,
        x=x_value,
        y=y_value,
        hue="task_name_cleaned",
        # marker="o",
        ax=ax,
    )
    ax.set_title(f"Flow {flow_name}: Task Fails Over Time")
    ax.set_xlabel(x_value.capitalize())
    ax.set_ylabel(y_value)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"{path}_error_count_over_time_{x_value}_{flow_name}.png"
    plt.savefig(plot_path)

    # line plot
    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(1, 1, 1)
    sns.lineplot(
        data=counts_df,
        x=x_value,
        y=y_value,
        hue="task_name_cleaned",
        marker="o",
        ax=ax,
        style="task_name_cleaned",
    )
    ax.set_title(f"Flow {flow_name}: Task Fails Over Time")
    ax.set_xlabel(x_value.capitalize())
    ax.set_ylabel(y_value)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plot_path = f"{path}_error_count_over_time_{x_value}_{flow_name}_line.png"
    plt.savefig(plot_path)
    logger.info(f"Success rate plot saved to {plot_path}")


def analyze_subflow_metrics(df, path, logger):
    print("analyzing subflow metrics")
    print(df.columns)
    df["task_name_cleaned"] = df["task_name"].str.split("-").str[0]
    df["task_total_run_time"] = df["task_total_run_time"].astype(float)
    df["task_run_time_in_min"] = df["task_total_run_time"] / 60
    all_run_time_stats = df.groupby(by=["task_name_cleaned"])[
        "run_time_in_min"
    ].describe()
    flow_run_time_stats = (
        df.groupby(by=["flow_name", "state_type"])["run_time_in_min"]
        .describe()
        .round(2)
    )
    flow_run_time_stats = flow_run_time_stats.reset_index()
    flow_run_time_stats.to_csv(f"{path}_flow_run_time_stats.csv", index=False)

    all_run_time_stats = (
        df.groupby(by=["flow_name", "task_name_cleaned", "task_state_type"])[
            "task_run_time_in_min"
        ]
        .describe()
        .round(2)
    )
    all_run_time_stats = all_run_time_stats.reset_index()
    all_run_time_stats.to_csv(f"{path}_all_run_time_stats.csv", index=False)

    # by year-week
    all_run_time_stats_by_year_week = (
        df.groupby(
            by=["flow_name", "task_name_cleaned", "year-week", "task_state_type"]
        )["task_run_time_in_min"]
        .describe()
        .round(2)
    )
    all_run_time_stats_by_year_week = all_run_time_stats_by_year_week.reset_index()
    all_run_time_stats_by_year_week.to_csv(
        f"{path}_all_run_time_stats_by_year_week.csv", index=False
    )
    # create boxplot
    for flow_name in df["flow_name"].unique():
        temp = df[df["flow_name"] == flow_name]
        temp["hue"] = temp["task_name_cleaned"] + "*" + temp["task_state_type"]
        mean_total_runtime = temp["task_run_time_in_min"].mean()
        std_total_runtime = temp["task_run_time_in_min"].std()
        fig = plt.figure(figsize=(12, 6))
        ax = fig.add_subplot(1, 1, 1)
        sns.boxplot(
            data=temp,
            x="year-week",
            y="task_run_time_in_min",
            hue="hue",
            ax=ax,
        )
        ax.set_title(
            f"Flow Run Time Over Time: {flow_name} - (M{mean_total_runtime:.2f} +/- {std_total_runtime:.2f})"
        )
        ax.set_xlabel("Year-Week")
        ax.set_ylabel("Run Time (min)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.tight_layout()
        plot_path = f"{path}_all_run_time_stats_by_year_week_boxplot_{flow_name}.png"
        plt.savefig(plot_path)
        logger.info(f"Success rate plot saved to {plot_path}")


def analyze_failed_flows(df, path, logger):
    df = df[(df["task_state_type"] == "FAILED") | (df["state_type"] == "FAILED")]
    df["state.message"] = df["state.message"].fillna("No error message available")
    df["task_name_cleaned"] = df["task_name"].str.split("-").str[0]
    logger.info("Task names: ")
    logger.info(df["task_name_cleaned"].unique())
    flow_names = df["flow_name"].unique()
    logger.info(f"Analyzing failed flows for flow names: {flow_names}")
    error_counts = (
        df.groupby(by=["state_type", "flow_name"])["task_name_cleaned"]
        .value_counts()
        .reset_index()
    )
    logger.info("Top error messages across all flows:")
    logger.info(error_counts.head(20))
    error_counts.to_csv(f"{path}_error_count_task_name.csv", index=False)

    error_counts_by_month = (
        df.groupby(by=["year-month", "flow_name"])["task_name_cleaned"]
        .value_counts()
        .reset_index()
    )
    error_counts_by_month = error_counts_by_month.reset_index()
    error_counts_by_month = error_counts_by_month.sort_values(
        by=["flow_name", "year-month", "task_name_cleaned"]
    )
    logger.info("Top error messages across all flows by month:")
    logger.info(error_counts_by_month.head(20))
    error_counts_by_month.to_csv(
        f"{path}_error_count_task_name_by_month.csv", index=False
    )

    error_counts_by_week = (
        df.groupby(by=["year-week", "flow_name"])["task_name_cleaned"]
        .value_counts()
        .reset_index()
    )
    error_counts_by_week = error_counts_by_week.reset_index()
    error_counts_by_week = error_counts_by_week.sort_values(
        by=["flow_name", "year-week", "task_name_cleaned"]
    )
    logger.info("Top error messages across all flows by week:")
    logger.info(error_counts_by_week.head(20))
    for flow_name in flow_names:
        error_counts_by_month_flow = error_counts_by_month[
            error_counts_by_month["flow_name"] == flow_name
        ]
        plot_error_counts_subtasks(
            error_counts_by_month_flow,
            path,
            logger,
            x_value="year-month",
            y_value="count",
            flow_name=flow_name,
        )
