#!/usr/bin/env python
"""
This script is for migrating jobs to another namespace
"""
import argparse
import subprocess
import time
from urllib.parse import urljoin
from urllib.parse import urlparse

import yaml

from tron.commands import client


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Migrate jobs to new namespace'
    )
    parser.add_argument(
        '--server',
        required=True,
        help='specify the location of tron master'
    )
    parser.add_argument(
        '--old-ns',
        required=True,
        help='Old namespace'
    )
    parser.add_argument(
        '--new-ns',
        required=True,
        help='New namespace'
    )
    parser.add_argument(
        'source',
        help='source file to get list of jobs'
    )
    parser.add_argument(
        '--job',
        help='Specify a single job to migrate'
    )
    args = parser.parse_args()
    return args


def check_job_if_running(jobs_status, job_name):
    for job_status in jobs_status:
        if job_status['name'] == job_name:
            status = job_status['status']
            if status == 'running':
                print(bcolors.FAIL + 'job {} is still running, can not migrate'.format(job_name) + bcolors.ENDC)
                return False
            else:
                print(bcolors.OKGREEN + 'job {} is not running, can migrate'.format(job_name) + bcolors.ENDC)
                return True

    print(bcolors.FAIL + 'Can not find the job {}'.format(job_name) + bcolors.ENDC)
    return False


def command_jobs(command, jobs, args, ns=None):
    """ This function run tronctl command for the jobs
    command: the tronctl command it will run
    jobs: a list of jobs
    args: the args for this script
    ns: the namespace to use as the prefix for each job, if None, the scrip would use args.old_ns instead
    """
    data = {'command': command}
    command_flag = True
    for job in jobs:
        if ns is not None:
            job_name = ns + '.' + job['name']
        else:
            job_name = args.old_ns + '.' + job['name']

        if command == 'move':
            data = {'command': command, 'old_name': args.old_ns + '.' + job['name'], 'new_name': args.new_ns + '.' + job['name']}
            uri = urljoin(args.server, 'api/jobs')
        else:
            data = {'command': command}
            uri = urljoin(args.server, 'api/jobs/' + job_name)

        response = client.request(uri, data=data)
        if response.error:
            print(bcolors.FAIL + 'Failed to {} {}'.format(command, job_name) + bcolors.ENDC)
            command_flag = False
        else:
            print(bcolors.OKGREEN + 'Succeed to {} {}'.format(command, job_name) + bcolors.ENDC)
    return command_flag


def ssh_command(hostname, command):
    print(bcolors.BOLD + 'Executing the command: ssh -A {} {}'.format(hostname, command) + bcolors.ENDC)
    ssh = subprocess.Popen(["ssh", "-A", hostname, command], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    exitcode = ssh.wait()
    result = ssh.stdout.readlines()
    error = ssh.stderr.readlines()
    if exitcode != 0:
        print(bcolors.FAIL + 'Execute command {} failed: {}'.format(command, error) + bcolors.ENDC)
        exit(exitcode)
    return result


def main():
    args = parse_args()
    filename = args.source
    hostname = urlparse(args.server).hostname
    if filename.endswith(".yaml"):
        tron_client = client.Client(args.server)
        jobs_status = tron_client.jobs()

        is_migration_safe = True
        with open(filename, "r") as f:
            jobs = yaml.load(f)['jobs']
            job_names = [job['name'] for job in jobs]
            if args.job is not None:  # only want to migrate specific job
                # Overwrite existing jobs since only migrating one job
                jobs = [job for job in jobs if job['name'] == args.job]
                if not jobs:
                    raise ValueError(f'Invalid job specified. Options were {job_names}')
                job_name_with_ns = args.old_ns + '.' + args.job
                is_migration_safe = is_migration_safe & check_job_if_running(jobs_status, job_name_with_ns)

            else:  # Migrate all jobs in namespace
                for job_name in job_names:
                    job_name_with_ns = args.old_ns + '.' + job_name
                    is_migration_safe = is_migration_safe & check_job_if_running(jobs_status, job_name_with_ns)

        if is_migration_safe is True:
            print(bcolors.OKBLUE + "Jobs are not running. Disable all the jobs." + bcolors.ENDC)
        else:
            print(bcolors.WARNING + "Some jobs are still running, abort this migration," + bcolors.ENDC)
            return

        # try stop cron
        ssh_command(hostname, "sudo service cron stop")

        # wait unitil yelpsoa-configs branch is merged
        res = input("Merge and push yelpsoa-configs branch. Ready to continue? [y/n]")
        if res == 'y':
            # wait for 10 seconds after pushing the branch
            time.sleep(30)
            # rsyn yelpsoa-configs
            command = "sudo rsync -a --delay-updates --contimeout=10 --timeout=10 --chmod=Du+rwx,go+rx --port=8731 --delete yelpsoa-slave.local.yelpcorp.com::yelpsoa-configs /nail/etc/services"
            ssh_command(hostname, command)

            # migrate jobs to new namespace
            command_jobs('move', jobs, args)

            # update new namespace
            ssh_command(hostname, "sudo paasta_setup_tron_namespace " + args.new_ns)

        #clean up namespace
        ssh_command(hostname, "sudo paasta_cleanup_tron_namespaces")

        # start cron
        ssh_command(hostname, "sudo service cron start")

    return


if __name__ == '__main__':
    main()
