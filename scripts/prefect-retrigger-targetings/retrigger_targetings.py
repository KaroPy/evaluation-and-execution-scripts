"""
Checks all k8s-targeting deployments in Prefect, evaluates their cron schedule
against today's date, and retriggers any that should have run today but have no
successful (COMPLETED) flow run.

Usage:
    python scripts/prefect-retrigger-targetings/retrigger_targetings.py [--dry-run] [--date YYYY-MM-DD]

Arguments:
    --dry-run   List the deployments that would be retriggered without triggering them.
    --date      Date to check (YYYY-MM-DD, defaults to today UTC).
"""

import argparse
import time
from datetime import date, datetime, timezone

from src.utils.logging_definitions import get_logger
from src.utils.prefect_api import call_prefect_api

FLOW_NAME_FILTER = "k8-targeting"
WAIT_SECONDS = 120
SUCCESSFUL_STATES = ["COMPLETED"]
FETCH_LIMIT = 200


# ---------------------------------------------------------------------------
# Cron helpers (no external deps)
# ---------------------------------------------------------------------------


def _matches_field(field: str, value: int, min_val: int, max_val: int) -> bool:
    """Return True if `value` satisfies the cron field expression."""
    for part in field.split(","):
        if "/" in part:
            range_part, step_str = part.split("/", 1)
            step = int(step_str)
            if range_part == "*":
                start, end = min_val, max_val
            elif "-" in range_part:
                start, end = map(int, range_part.split("-", 1))
            else:
                start, end = int(range_part), max_val
            if value in range(start, end + 1, step):
                return True
        elif "-" in part:
            start, end = map(int, part.split("-", 1))
            if start <= value <= end:
                return True
        elif part == "*":
            return True
        else:
            if int(part) == value:
                return True
    return False


def _day_matches(cron_expr: str, target: date) -> bool:
    """Return True if the cron's date fields (dom/month/dow) match `target`."""
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return False
    _, _, dom, month, dow = parts

    if not _matches_field(month, target.month, 1, 12):
        return False

    dom_match = _matches_field(dom, target.day, 1, 31)
    cron_dow_value = (target.weekday() + 1) % 7  # Mon=0…Sun=6 → Sun=0…Sat=6
    dow_match = _matches_field(dow, cron_dow_value, 0, 6)

    dom_is_star = dom == "*"
    dow_is_star = dow == "*"
    if dom_is_star and dow_is_star:
        return True
    if dom_is_star:
        return dow_match
    if dow_is_star:
        return dom_match
    return dom_match or dow_match


def cron_fire_times_on_date(cron_expr: str, target: date) -> list[datetime]:
    """
    Return every UTC datetime the cron expression fires on `target`.

    Supports standard 5-field cron (minute hour dom month dow).
    """
    if not _day_matches(cron_expr, target):
        return []
    parts = cron_expr.strip().split()
    minute_field, hour_field = parts[0], parts[1]
    fire_times = []
    for hour in range(24):
        if not _matches_field(hour_field, hour, 0, 23):
            continue
        for minute in range(60):
            if not _matches_field(minute_field, minute, 0, 59):
                continue
            fire_times.append(
                datetime(
                    target.year,
                    target.month,
                    target.day,
                    hour,
                    minute,
                    tzinfo=timezone.utc,
                )
            )
    return fire_times


def extract_cron_from_deployment(deployment: dict) -> str | None:
    """
    Return the first active cron expression found on a deployment, or None.

    Handles both Prefect v2 `schedules` list and legacy single `schedule` field.
    """
    # Prefect v2: list of schedule objects
    schedules = deployment.get("schedules") or []
    for sched_obj in schedules:
        if not sched_obj.get("active", True):
            continue
        sched = sched_obj.get("schedule") or {}
        cron = sched.get("cron")
        if cron:
            return cron

    # Prefect v2 legacy / single schedule
    sched = deployment.get("schedule") or {}
    cron = sched.get("cron")
    return cron if cron else None


# ---------------------------------------------------------------------------
# Prefect API helpers
# ---------------------------------------------------------------------------


def fetch_targeting_deployments(logger) -> list[dict]:
    logger.info(f"Fetching all deployments matching '{FLOW_NAME_FILTER}'")
    endpoint = "/deployments/filter"
    payload = {
        "deployments": {"name": {"like_": FLOW_NAME_FILTER}},
        "limit": FETCH_LIMIT,
    }
    response = call_prefect_api(endpoint, payload)
    logger.info(f"Found {len(response)} deployments")
    return response


def fetch_successful_runs_today(deployment_id: str, day_start: str, day_end: str) -> list[dict]:
    endpoint = "/flow_runs/filter"
    payload = {
        "flow_runs": {
            "deployment_id": {"any_": [deployment_id]},
            "state": {"type": {"any_": SUCCESSFUL_STATES}},
            "start_time": {
                "after_": day_start,
                "before_": day_end,
            },
        },
        "limit": FETCH_LIMIT,
    }
    return call_prefect_api(endpoint, payload)


def trigger_flow_run(deployment_id: str, parameters: dict) -> dict:
    endpoint = f"/deployments/{deployment_id}/create_flow_run"
    return call_prefect_api(endpoint, {"parameters": parameters})


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------


def resolve_date(date_str: str | None) -> date:
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date()


def day_bounds(target: date) -> tuple[str, str]:
    return (
        target.strftime("%Y-%m-%dT00:00:00Z"),
        target.strftime("%Y-%m-%dT23:59:59Z"),
    )


def deployments_needing_retrigger(
    deployments: list[dict],
    target: date,
    day_start: str,
    day_end: str,
    now: datetime,
    logger,
) -> list[tuple[dict, datetime]]:
    due = []
    for dep in deployments:
        name = dep.get("name", dep.get("id"))
        cron = extract_cron_from_deployment(dep)
        if not cron:
            logger.info(f"  SKIP {name}: no cron schedule found")
            continue

        past_fires = [t for t in cron_fire_times_on_date(cron, target) if t <= now]
        if not past_fires:
            fire_times = cron_fire_times_on_date(cron, target)
            next_fire = fire_times[0].strftime("%H:%M UTC") if fire_times else "never"
            logger.info(f"  SKIP {name}: cron '{cron}' has not fired yet (next: {next_fire})")
            continue

        scheduled_at = past_fires[-1]
        dep_id = dep["id"]
        successful = fetch_successful_runs_today(dep_id, day_start, day_end)
        if successful:
            logger.info(f"  OK   {name}: {len(successful)} successful run(s) found today")
            continue

        logger.info(
            f"  NEED {name}: cron '{cron}' last fired at {scheduled_at.strftime('%H:%M UTC')},"
            " no successful run found"
        )
        due.append((dep, scheduled_at))

    return due


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Retrigger k8s-targeting deployments whose cron schedule fires today"
            " but have no successful run."
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List runs to be retriggered without triggering.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date to check (YYYY-MM-DD, defaults to today UTC).",
    )
    args = parser.parse_args()

    logger = get_logger(
        f"scripts/prefect-retrigger-targetings/retrigger_targetings-{datetime.now()}"
    )

    now = datetime.now(timezone.utc)
    target = resolve_date(args.date)
    day_start, day_end = day_bounds(target)
    logger.info(f"Checking date: {target}, now: {now.strftime('%Y-%m-%dT%H:%M:%SZ')}")

    deployments = fetch_targeting_deployments(logger)
    if not deployments:
        print("No k8s-targeting deployments found.")
        return

    due = deployments_needing_retrigger(deployments, target, day_start, day_end, now, logger)

    if not due:
        print(
            f"All k8s-targeting deployments due before {now.strftime('%H:%M UTC')} ran successfully."
        )
        return

    if args.dry_run:
        print(f"[DRY RUN] {len(due)} deployment(s) would be retriggered for {target}:")
        for i, (dep, scheduled_at) in enumerate(due):
            cron = extract_cron_from_deployment(dep)
            tenant = dep.get("parameters", {}).get("tenant", "?")
            audience = dep.get("parameters", {}).get("audience", "?")
            print(
                f"  [{i + 1}] {dep.get('name')}  cron='{cron}'"
                f"  scheduled={scheduled_at.strftime('%H:%M UTC')}"
                f"  tenant={tenant}  audience={audience}"
            )
        return
    print(
        f"Retriggering {len(due)} deployment(s) for {target} (waiting {WAIT_SECONDS}s between each):"
    )
    for i, (dep, scheduled_at) in enumerate(due):
        dep_id = dep["id"]
        dep_name = dep.get("name", dep_id)
        parameters = dep.get("parameters") or {}
        cron = extract_cron_from_deployment(dep)
        tenant = parameters.get("tenant", "?")
        audience = parameters.get("audience", "?")
        print(
            f"  [{i + 1}] {dep_name}  cron='{cron}'"
            f"  scheduled={scheduled_at.strftime('%H:%M UTC')}"
            f"  tenant={tenant}  audience={audience}"
        )
        try:
            new_run = trigger_flow_run(dep_id, parameters)
            logger.info(f"Triggered flow_run_id={new_run.get('id')} for {dep_name}")
            print(f"    -> Triggered: flow_run_id={new_run.get('id')}")
        except Exception as exc:
            logger.exception(f"Failed to trigger {dep_name}")
            print(f"    -> ERROR: {exc}")

        if i < len(due) - 1:
            logger.info(f"Waiting {WAIT_SECONDS}s before next trigger …")
            time.sleep(WAIT_SECONDS)

    print("Done.")


if __name__ == "__main__":
    main()
