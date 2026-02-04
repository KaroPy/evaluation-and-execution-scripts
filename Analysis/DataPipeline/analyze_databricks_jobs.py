import argparse
import pandas as pd
from datetime import datetime
from src.databricks.query_databricks_jobs import (
    list_jobs,
    get_jobs_with_task_states,
    summarize_task_states,
)
from src.databricks.return_stats import count_failed_and_success
from src.utils.logging_definitions import get_logger

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Query Databricks jobs and task states"
    )
    parser.add_argument(
        "--days", type=int, default=7, help="Number of days to look back (default: 7)"
    )
    parser.add_argument("--job-id", type=int, help="Filter by specific job ID")
    parser.add_argument(
        "--list-jobs", action="store_true", help="List all jobs in the workspace"
    )
    parser.add_argument("--output", type=str, help="Output CSV file path")
    parser.add_argument(
        "--summary", action="store_true", help="Show summary statistics"
    )
    parser.add_argument(
        "--include-output",
        action="store_true",
        help="Include task output/error messages (slower, makes additional API calls)",
    )
    parser.add_argument(
        "--include-trial-runs",
        action="store_true",
        help="Include trial/retry run information",
    )

    args = parser.parse_args()

    logger = get_logger(f"analyze_databricks_jobs_{datetime.now()}")

    if args.list_jobs:
        logger.info("Listing all jobs...")
        jobs_df = list_jobs()
        logger.info(jobs_df.to_string())
    else:
        try:
            df = pd.read_csv(
                f"Analysis/DataPipeline/data/{args.output}/{args.output}_databrick_runs_{args.days}.csv"
            )
            logger.info("Loaded job runs from existing CSV file.")
        except FileNotFoundError:
            logger.info("Querying Databricks jobs...")
            df = get_jobs_with_task_states(
                days=args.days,
                job_id=args.job_id,
                include_output=args.include_output,
                include_trial_runs=args.include_trial_runs,
            )

            if df.empty:
                logger.info("No job runs found")
            else:
                if args.summary:
                    summary = summarize_task_states(df)
                    logger.info("\n=== Task State Summary ===")
                    logger.info(summary.to_string())
                else:
                    logger.info("\n=== Task States ===")
                    display_cols = [
                        "run_name",
                        "task_key",
                        "life_cycle_state",
                        "result_state",
                        "start_time",
                        "execution_duration_sec",
                    ]
                    # Add error column if output was included
                    if args.include_output:
                        display_cols.append("error")
                    # Add trial run indicator if trial runs were included
                    if args.include_trial_runs:
                        display_cols.insert(3, "is_trial_run")
                    available_cols = [c for c in display_cols if c in df.columns]
                    logger.info(df[available_cols].to_string())

                if args.output:
                    df.to_csv(
                        f"Analysis/DataPipeline/data/{args.output}/{args.output}_databrick_runs_{args.days}.csv",
                        index=False,
                    )
                    logger.info(f"\nResults saved to {args.output}")
    count_failed_and_success(
        df,
        f"Analysis/DataPipeline/data/{args.output}/{args.output}_databrick_runs_{args.days}",
        logger=logger,
    )

# python Analysis/DataPipeline/analyze_databricks_jobs.py.py --days 270 --include-output --include-trial-runs --output 2026-01-15
