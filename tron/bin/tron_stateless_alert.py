#!/usr/bin/env python3.6
import argparse
import logging
import os
import time

from tron.config import manager
from tron.config import schema
from tron.serialize.runstate.dynamodb_state_store import DynamoDBStateStore

# Default values for arguments
DEFAULT_WORKING_DIR = '/var/lib/tron/'
DEFAULT_CONF_PATH = 'config/'
DEFAULT_ALERT_AFTER = 30
log = logging.getLogger('tron_stateless_alert')


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
    timestamps.sort()
    return timestamps[-1] if timestamps else None


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
        "--job-for-stateless-alert",
        required=True,
        help="The job name to read timestamp from",
    )
    parser.add_argument(
        "--alert-after",
        default=DEFAULT_ALERT_AFTER,
        help="how long (in mins) to wait to alert after the last timestamp of the job",
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


def calc_time_diff(last_run_time, now):
    def trim_nanoseconds(timestamp):
        index = timestamp.find('.')
        if index != -1:
            timestamp = timestamp[:index]
        return timestamp

    def _timestamp_to_timeobj(timestamp):
        return time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

    time_diff = (
        now -
        time.mktime(_timestamp_to_timeobj(trim_nanoseconds(str(last_run_time))))
    )
    # secs to mins
    return time_diff / 60


def alert(check_every, stateless_for_mins):
    import pysensu_yelp
    result_dict = {
        'name': 'tron_stateless_alert',
        'runbook': 'y/rb-tron',
        'status': 1,
        'output': 'DynamoDB has not been updated for {} mins'.format(stateless_for_mins),
        'team': 'compute-infra',
        'tip': '',
        'page': False,
        'notification_email': 'mingqiz@yelp.com',
        'irc_channels': None,
        'slack_channels': None,
        'check_every': '{}s'.format(check_every * 60)
    }
    pysensu_yelp.send_event(**result_dict)


def main():
    # Fetch configs. You can find the arguments in puppet.
    args = parse_cli()
    persistence_config = read_config(args)
    store_type = schema.StatePersistenceTypes(persistence_config.store_type)
    job_for_stateless_alert = args.job_for_stateless_alert

    # Make sure datastore is dynamodb
    if store_type == schema.StatePersistenceTypes.dynamodb:
        dynamodb_region = persistence_config.dynamodb_region
        table_name = persistence_config.table_name
        # Fetch job state from dynamodb
        store = DynamoDBStateStore(table_name, dynamodb_region)
        key = store.build_key('job_state', '{}'.format(job_for_stateless_alert))
        job = store.restore([key])[key]
        # Alert if timestamp is not updated after alert_after mins
        # Skip the check if the job never runs. This is because the intended job used as
        # timestamp is paasta-contract-monitor, which alerts if it does not run
        last_run_time = get_last_run_time(job)
        if last_run_time:
            stateless_for_mins = calc_time_diff(last_run_time, time.time())
            if stateless_for_mins > args.alert_after:
                alert(args.alert_after, stateless_for_mins)


if __name__ == '__main__':
    main()
