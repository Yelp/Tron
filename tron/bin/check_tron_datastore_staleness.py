#!/usr/bin/env python3.6
import argparse
import logging
import os
import sys
import time

import pytz

from tron.config import manager
from tron.config import schema
from tron.serialize.runstate.statemanager import PersistenceManagerFactory

# Default values for arguments
DEFAULT_WORKING_DIR = '/var/lib/tron/'
DEFAULT_CONF_PATH = 'config/'
DEFAULT_STALENESS_THRESHOLD = 1800
log = logging.getLogger('check_tron_datastore_staleness')


def get_last_run_time(job):
    '''
    Get all sorted timestamps, and only count the actions that actually ran
    '''
    timestamps = []
    job_runs = job['runs']
    for run in job_runs:
        for action in run['runs']:
            if action.get('start_time') and action.get('state') != 'scheduled':
                timestamps.append(action.get('start_time'))
    return max(timestamps) if timestamps else None


def parse_cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--working-dir",
        default=DEFAULT_WORKING_DIR,
        help="Working directory for the Tron daemon, default %(default)s",
    )
    parser.add_argument(
        "-c",
        "--config-path",
        default=DEFAULT_CONF_PATH,
        help="File path to the Tron configuration file",
    )
    parser.add_argument(
        "--job-name",
        required=True,
        help="The job name to read timestamp from",
    )
    parser.add_argument(
        "--staleness-threshold",
        default=DEFAULT_STALENESS_THRESHOLD,
        help="how long (in seconds) to wait to alert after the last timestamp of the job",
    )
    args = parser.parse_args()
    args.working_dir = os.path.abspath(args.working_dir)
    args.config_path = os.path.join(
        args.working_dir,
        args.config_path,
    )
    return args


def read_config(args):
    return manager.ConfigManager(args.config_path).load().get_master().state_persistence


def main():
    # Fetch configs. You can find the arguments in puppet.
    args = parse_cli()
    persistence_config = read_config(args)
    store_type = schema.StatePersistenceTypes(persistence_config.store_type)
    job_name = args.job_name

    # Alert for DynamoDB
    if store_type == schema.StatePersistenceTypes.dynamodb:
        # Fetch job state from dynamodb
        state_manager = PersistenceManagerFactory.from_config(persistence_config)
        try:
            job = state_manager.restore(job_names=[job_name])['job_state'][job_name]
        except Exception as e:
            logging.exception(f'UNKN: Failed to retreive status for job {job_name} due to {e}')
            sys.exit(3)

        # Exit if the job never runs.
        last_run_time = get_last_run_time(job)
        if not last_run_time:
            logging.error(f'WARN: No last run for {job_name} found. If the job was just added, it might take some time for it to run')
            sys.exit(1)

        # Alert if timestamp is not updated after staleness_threshold
        stateless_for_secs = time.time() - last_run_time.astimezone(pytz.utc).timestamp()
        if stateless_for_secs > args.staleness_threshold:
            logging.error(f'CRIT: {job_name} has not been updated in DynamoDB for {stateless_for_secs} seconds')
            sys.exit(2)
        else:
            logging.info(f"OK: DynamoDB is up to date. It's last updated at {last_run_time}")
            sys.exit(0)
    # Alert for BerkeleyDB
    elif store_type == schema.StatePersistenceTypes.shelve:
        os.execl('/usr/lib/nagios/plugins/check_file_age', '/nail/tron/tron_state', '-w', str(args.staleness_threshold), '-c', str(args.staleness_threshold))
    else:
        logging.exception(f'UNKN: Not designed to check this type of datastore: {store_type}')
        sys.exit(3)


if __name__ == '__main__':
    main()
