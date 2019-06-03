#!/usr/bin/env python3.6
import argparse
import logging
import os
import sys
import time

import pytz

from tron.config import manager
from tron.config import schema
from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore

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
        dynamodb_region = persistence_config.dynamodb_region
        table_name = persistence_config.table_name
        store = DynamoDBStateStore(table_name, dynamodb_region)
        key = store.build_key('job_state', job_name)
        try:
            job = store.restore([key])[key]
        except Exception as e:
            logging.exception(f'Failed to retreive status for job {job_name} due to {e}')
            sys.exit(1)

        # Exit if the job never runs.
        last_run_time = get_last_run_time(job)
        if not last_run_time:
            logging.error(f'No last run for {key} found. If the job was just added, it might take some time for it to run')
            sys.exit(1)

        # Alert if timestamp is not updated after staleness_threshold
        stateless_for_secs = time.time() - last_run_time.astimezone(pytz.utc).timestamp()
        if stateless_for_secs > args.staleness_threshold:
            logging.error(f'{key} has not been updated in DynamoDB for {stateless_for_secs} seconds')
            sys.exit(1)
        logging.info(f"DynamoDB is up to date. It's last updated at {last_run_time}")
    # Alert for BerkeleyDB
    elif store_type == schema.StatePersistenceTypes.shelve:
        os.execl('/usr/lib/nagios/plugins/check_file_age', '/nail/tron/tron_state', '-w', str(args.staleness_threshold), '-c', str(args.staleness_threshold))

    sys.exit(0)


if __name__ == '__main__':
    main()
