#!/usr/bin/env python
import argparse
import logging
import os
from datetime import datetime

import boto3

from tron.config import manager
from tron.config import schema

# Default values for arguments
DEFAULT_WORKING_DIR = '/var/lib/tron/'
DEFAULT_CONF_PATH = 'config/'
MAX_BACKUPS = 5
log = logging.getLogger('ddbbackup')


def create_backup(ddb, table_name):
    backup_name = '{}-{}'.format(table_name, datetime.now().strftime('%Y%m%d%H%M'))
    log.debug("Creating backup:", backup_name)
    response = ddb.create_backup(
        TableName=table_name, BackupName=backup_name)
    log.debug(response)


def delete_old_backups(ddb, table_name, max_backups):
    log.debug("Deleting old backups for table:", table_name)

    backups = ddb.list_backups(TableName=table_name)

    backup_count = len(backups['BackupSummaries'])
    log.debug('Total backup count:', backup_count)

    if backup_count <= max_backups:
        print("No stale backups. Exiting.")
        return

    sorted_list = sorted(backups['BackupSummaries'],
                         key=lambda k: k['BackupCreationDateTime'])

    old_backups = sorted_list[:max_backups]

    for backup in old_backups:
        arn = backup['BackupArn']
        log.debug("ARN to delete: " + arn)
        deleted_arn = ddb.delete_backup(BackupArn=arn)
        status = deleted_arn['BackupDescription']['BackupDetails']['BackupStatus']
        log.debug("Status:", status)

    return


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
        "-m",
        "--max-backups",
        default=MAX_BACKUPS,
        help="The maximum number of backups",
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

    # If backup only if datastore is dynamodb
    store_type = schema.StatePersistenceTypes(persistence_config.store_type)
    if store_type == schema.StatePersistenceTypes.dynamodb:
        #set up dynamodb client
        region_name = persistence_config.dynamodb_region
        table_name = persistence_config.table_name
        max_backups = args.max_backups
        ddb = boto3.client('dynamodb', region_name=region_name)
        #create backup and remove old ones
        create_backup(ddb, table_name)
        delete_old_backups(ddb, table_name, max_backups)


if __name__ == '__main__':
    main()
