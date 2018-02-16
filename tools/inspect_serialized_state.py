"""Read a state file or db and create a report which summarizes it's contents.

Displays:
State configuration
Count of jobs
Count of services

Table of Jobs with start date of last run
Table of Services with state and instance count

"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import optparse

from tron.config import manager
from tron.serialize.runstate import statemanager
from tron.utils import tool_utils


def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("-c", "--config-path", help="Path to the configuration.")
    parser.add_option(
        "-w", "--working-dir", default=".",
        help="Working directory to resolve relative paths.",
    )
    opts, _ = parser.parse_args()

    if not opts.config_path:
        parser.error("A --config-path is required.")
    return opts


def get_container(config_path):
    config_manager = manager.ConfigManager(config_path)
    return config_manager.load()


def get_state(container):
    config = container.get_master().state_persistence
    state_manager = statemanager.PersistenceManagerFactory.from_config(config)
    names = container.get_job_and_service_names()
    return state_manager.restore(*names)


def format_date(date_string):
    return date_string.strftime("%Y-%m-%d %H:%M:%S") if date_string else None


def format_jobs(job_states):
    format = "%-30s %-8s %-5s %s\n"
    header = format % ("Name", "Enabled", "Runs", "Last Update")

    def max_run(item):
        start_time = filter(None, (run['start_time'] for run in item))
        return max(start_time) if start_time else None

    def build(name, job):
        start_times = (max_run(job_run['runs']) for job_run in job['runs'])
        start_times = filter(None, start_times)
        last_run = format_date(max(start_times)) if start_times else None
        return format % (name, job['enabled'], len(job['runs']), last_run)
    seq = sorted(build(*item) for item in job_states.iteritems())
    return header + "".join(seq)


def format_service(service_states):
    format = "%-30s %-8s %s\n"
    header = format % ("Name", "Enabled", "Instances")

    def build(name, service):
        return format % (name, service.get('enabled'), len(service['instances']))
    seq = sorted(build(*item) for item in service_states.iteritems())
    return header + "".join(seq)


def display_report(state_config, job_states, service_states):
    print("State Config: %s" % str(state_config))
    print("Total Jobs: %s" % len(job_states))
    print("Total Services: %s" % len(service_states))

    print("\n%s" % format_jobs(job_states))
    print("\n%s" % format_service(service_states))


def main(config_path, working_dir):
    container = get_container(config_path)
    config = container.get_master().state_persistence
    with tool_utils.working_dir(working_dir):
        display_report(config, *get_state(container))


if __name__ == "__main__":
    opts = parse_options()
    main(opts.config_path, opts.working_dir)
