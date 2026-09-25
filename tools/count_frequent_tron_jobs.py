"""
List Tron jobs whose schedule fires at or under a given frequency threshold.

The script takes two arguments:

It pulls data from the Tron API and analyzes the cron schedules for each job in the response.

Arguments:
    --minutes: The threshold in minutes (inclusive)
    --cluster: The Tron cluster to query (default: pnw-prod)

    --url: Override base URL for the Tron API (this one is optional, we can use it to test with the other Tron clusters)

Example usage:

For prod:
    python count_frequent_tron_jobs.py --minutes 30 --cluster pnw-prod
    (you could skip the --cluster argument and just use minutes for easier testing)
For devc cluster:
    python count_frequent_tron_jobs.py --minutes 30 --cluster pnw-devc

For testing with the other Tron clusters:
    python count_frequent_tron_jobs.py --minutes 30 --url http://tron-pnw-infrastage.yelpcorp.com
"""
import argparse
import sys

import requests


CLUSTER_URLS = {
    "pnw-prod": "https://tron-pnw-prod-api.yelpcorp.com",
    "pnw-devc": "http://tron-pnw-devc.yelpcorp.com",
}


def expand_cron_field(field_str, min_val, max_val):
    """Expand a cron field string to a sorted list of concrete int values."""
    values = set()
    for part in field_str.split(","):
        if part == "*":
            values.update(range(min_val, max_val + 1))
        elif part.startswith("*/"):
            step = int(part[2:])
            values.update(range(min_val, max_val + 1, step))
        elif "/" in part:
            range_part, step = part.split("/", 1)
            step = int(step)
            if "-" in range_part:
                start, end = range_part.split("-", 1)
                values.update(range(int(start), int(end) + 1, step))
            else:
                values.update(range(int(range_part), max_val + 1, step))
        elif "-" in part:
            start, end = part.split("-", 1)
            values.update(range(int(start), int(end) + 1))
        else:
            values.add(int(part))
    return sorted(values)


CRON_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def cron_frequency_minutes(cron_expr):
    """This computes and returns the minimum interval in minutes between consecutive fires of a cron expression."""
    expr = cron_expr.strip()
    if expr in CRON_ALIASES:
        expr = CRON_ALIASES[expr]

    parts = expr.split()
    if len(parts) != 5:
        return None

    minute_field, hour_field = parts[0], parts[1]

    minutes = expand_cron_field(minute_field, 0, 59)
    hours = expand_cron_field(hour_field, 0, 23)

    if not minutes or not hours:
        return None

    fire_times = sorted(h * 60 + m for h in hours for m in minutes)

    if len(fire_times) == 1:
        return 1440

    gaps = [
        gap
        for i in range(len(fire_times))
        if (gap := (fire_times[(i + 1) % len(fire_times)] - fire_times[i]) % 1440) > 0
    ]

    return min(gaps) if gaps else 1440


def normalize_schedule(schedule):
    """
    Return (schedule_type, cron_expr_or_None, raw_string).
    schedule_type were taken from the original script that parsed from yelpsoa-configs repo
    """
    if schedule is None or schedule == "":
        return ("disabled", None, str(schedule))

    if isinstance(schedule, str):
        raw = schedule
        if schedule.startswith("cron "):
            return ("cron", schedule[5:].strip(), raw)
        elif schedule.startswith("daily "):
            return ("daily", None, raw)
        else:
            return ("groc", None, raw)

    if isinstance(schedule, dict):
        stype = schedule.get("type", "")
        if stype == "cron":
            value = schedule.get("value", "")
            return ("cron", value, f"cron {value}")
        elif stype == "daily":
            return ("daily", None, str(schedule))
        elif "start_time" in schedule:
            return ("daily", None, str(schedule))
        else:
            return ("groc", None, str(schedule))

    return ("groc", None, str(schedule))


def compute_frequency(schedule):
    """Return frequency in minutes, or None if cannot be computed. We return 1440 for any schedule that goes over a day"""
    stype, cron_expr, _ = normalize_schedule(schedule)
    if stype == "disabled":
        return None
    if stype in ("daily", "groc"):
        return 1440
    freq = cron_frequency_minutes(cron_expr)
    if stype is None:
        print(f"Unknown schedule type: {schedule}")
    return freq


def fetch_jobs_from_api(base_url):
    """Fetch all jobs from the Tron API with minimal payload."""
    url = f"{base_url}/api/jobs?include_job_runs=0&include_action_runs=0&include_action_graph=0&include_node_pool=0"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching jobs from {base_url}: {e}", file=sys.stderr)
        sys.exit(1)
    return response.json()["jobs"]


def main():
    parser = argparse.ArgumentParser(
        description="List Tron jobs firing at or under a frequency threshold.",
        epilog="Example: %(prog)s --cluster pnw-prod --minutes 30",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        required=True,
        help="Threshold in minutes (inclusive)",
    )
    parser.add_argument(
        "--cluster",
        choices=CLUSTER_URLS.keys(),
        default="pnw-prod",
        help="Tron cluster to query (default: pnw-prod)",
    )
    parser.add_argument(
        "--url",
        help="Override base URL for the Tron API (takes precedence over --cluster)",
    )
    args = parser.parse_args()

    base_url = args.url if args.url else CLUSTER_URLS[args.cluster]

    jobs = fetch_jobs_from_api(base_url)

    results = []
    for job in jobs:
        name = job["name"]
        scheduler = job.get("scheduler")
        freq = compute_frequency(scheduler)
        if freq is not None and freq <= args.minutes:
            _, _, raw = normalize_schedule(scheduler)
            namespace, _, job_name = name.partition(".")
            results.append((freq, namespace, job_name, raw))

    results.sort(key=lambda r: (r[0], r[1], r[2]))

    if not results:
        print(f"No jobs found firing every {args.minutes} minutes or less.")
        return

    col_ns = max(len("NAMESPACE"), max(len(r[1]) for r in results))
    col_job = max(len("JOB NAME"), max(len(r[2]) for r in results))
    col_sched = max(len("SCHEDULE"), max(len(r[3]) for r in results))
    col_freq = len("FREQ (min)")

    header = (
        f"{'NAMESPACE':<{col_ns}}  {'JOB NAME':<{col_job}}  " f"{'SCHEDULE':<{col_sched}}  {'FREQ (min)':<{col_freq}}"
    )
    print(header)
    print("-" * len(header))

    for freq, namespace, job_name, raw_sched in results:
        print(f"{namespace:<{col_ns}}  {job_name:<{col_job}}  " f"{raw_sched:<{col_sched}}  {freq:<{col_freq}}")

    print(f"\nTotal: {len(results)} job(s) fire every {args.minutes} minutes or less.")
    print(f"Total jobs: {len(jobs)}")


if __name__ == "__main__":
    main()
