"""
List Tron jobs whose schedule fires at or under a given frequency threshold.

The script takes two arguments:

--minutes: The threshold in minutes (inclusive)
--repo: path to yelpsoa-configs (the script goes through all tron-*.yaml files under the repo)

both arguments are required.
"""
import argparse
import glob
import os
import sys

import yaml


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
    """Return the minimum interval in minutes between consecutive fires of a cron expression."""
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

    # Build all (hour, minute) fire times in a 24h window, sorted
    fire_times = sorted(h * 60 + m for h in hours for m in minutes)

    if len(fire_times) == 1:
        return 1440  # fires once per day

    # Minimum gap between consecutive fires (wrapping midnight)
    gaps = [
        gap
        for i in range(len(fire_times))
        if (gap := (fire_times[(i + 1) % len(fire_times)] - fire_times[i]) % 1440) > 0
    ]

    return min(gaps) if gaps else 1440


def normalize_schedule(schedule):
    """
    Return (schedule_type, cron_expr_or_None, raw_string).
    schedule_type is one of: 'cron', 'daily', 'groc', 'disabled'.
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
    """Return frequency in minutes, or None if disabled/unparseable."""
    stype, cron_expr, _ = normalize_schedule(schedule)
    if stype == "disabled":
        return None
    if stype in ("daily", "groc"):
        return 1440
    freq = cron_frequency_minutes(cron_expr)
    return freq


def find_tron_files(repo_root):
    pattern = os.path.join(repo_root, "*/tron-*.yaml")
    return glob.glob(pattern)


def main():
    parser = argparse.ArgumentParser(
        description="List Tron jobs firing at or under a frequency threshold.",
        epilog="Example: %(prog)s --repo /path/to/yelpsoa-configs --minutes 30",
    )
    parser.add_argument(
        "--minutes",
        type=int,
        required=True,
        help="Threshold in minutes (inclusive)",
    )
    parser.add_argument(
        "--repo",
        required=True,
        help="Path to yelpsoa-configs root (the directory containing service subdirectories)",
    )
    args = parser.parse_args()

    tron_files = find_tron_files(args.repo)
    if not tron_files:
        print(f"No tron-*.yaml files found under {args.repo}", file=sys.stderr)
        sys.exit(1)

    results = []
    for filepath in sorted(tron_files):
        service = os.path.basename(os.path.dirname(filepath))
        try:
            with open(filepath) as f:
                data = yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: could not parse {filepath}: {e}", file=sys.stderr)
            continue

        if not isinstance(data, dict):
            continue

        for key, job in data.items():
            if key == "jobs" and isinstance(job, dict):
                # tron-<cluster>.yaml top-level 'jobs:' dict
                for job_name, job_def in job.items():
                    if not isinstance(job_def, dict):
                        continue
                    schedule = job_def.get("schedule")
                    freq = compute_frequency(schedule)
                    if freq is not None and freq <= args.minutes:
                        _, _, raw = normalize_schedule(schedule)
                        results.append((freq, service, job_name, raw))
            elif isinstance(job, dict) and "schedule" in job:
                # direct job dict at top level
                schedule = job.get("schedule")
                freq = compute_frequency(schedule)
                if freq is not None and freq <= args.minutes:
                    _, _, raw = normalize_schedule(schedule)
                    results.append((freq, service, key, raw))

    seen = {}
    for freq, service, job_name, raw in results:
        dedup_key = (service, job_name)
        if dedup_key not in seen or freq < seen[dedup_key][0]:
            seen[dedup_key] = (freq, raw)
    results = [(freq, svc, job, raw) for (svc, job), (freq, raw) in seen.items()]

    results.sort(key=lambda r: (r[0], r[1], r[2]))

    if not results:
        print(f"No jobs found firing every {args.minutes} minutes or less.")
        return

    col_svc = max(len("SERVICE"), max(len(r[1]) for r in results))
    col_job = max(len("JOB NAME"), max(len(r[2]) for r in results))
    col_sched = max(len("SCHEDULE"), max(len(r[3]) for r in results))
    col_freq = len("FREQ (min)")

    header = (
        f"{'SERVICE':<{col_svc}}  {'JOB NAME':<{col_job}}  " f"{'SCHEDULE':<{col_sched}}  {'FREQ (min)':<{col_freq}}"
    )
    print(header)
    print("-" * len(header))

    for freq, service, job_name, raw_sched in results:
        print(f"{service:<{col_svc}}  {job_name:<{col_job}}  " f"{raw_sched:<{col_sched}}  {freq:<{col_freq}}")

    print(f"\nTotal: {len(results)} job(s) fire every {args.minutes} minutes or less.")


if __name__ == "__main__":
    main()
