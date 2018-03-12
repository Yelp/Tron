#!/usr/bin/env python
"""
Find processes missing from Tron.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json
import optparse
import subprocess
import sys
import time
import traceback
from fnmatch import fnmatch
from multiprocessing.pool import ThreadPool
from tempfile import TemporaryFile

import six
from six import string_types
from six.moves.urllib import request


DEFAULT_SIGNAL = 'TERM'
DEFAULT_KILL_PREFIX = ''
DEFAULT_MAX_THREADS = 64


COMMANDS = (
    'list',
    'kill',
)


def _print_event(contents):
    contents.setdefault('time', time.time())
    print(json.dumps(contents))


def _ssh_atoms(host, user, forward_ssh_agent):
    result = ['ssh']

    if forward_ssh_agent:
        result.append('-A')

    if user:
        host = '@'.join([user, host])
    result.append(host)

    return result


def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')

    if 'stdin' in kwargs:
        curr = kwargs['stdin']
        if isinstance(curr, string_types):
            f = TemporaryFile()
            f.write(curr)
            f.seek(0)
            kwargs['stdin'] = f

    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd)
    return output


def pidfiles_for_host(hostname, services):
    return [
        (
            service['name'],
            service['pid_filename'] % {
                'name': service['name'],
                'instance_number': '.*',
            },
            [
                service['pid_filename'] % {
                    'name': service['name'],
                    'instance_number': index,
                }
                for index, inst in enumerate(service['instances'])
                if inst['node']['hostname'] == hostname
            ],
        )
        for service in services
    ]


def ssh_get_instances(host, service_to_target, user, forward_ssh_agent):
    remote_output = _check_output(
        _ssh_atoms(host, user, forward_ssh_agent) + ['bash'],

        stdin="""
            OUT_LOCK=$(mktemp)
            declare -A services=( %(bash_hash)s )
            for service in "${!services[@]}"
            do
                (
                    RESULT=$(
                        echo $service
                        pgrep -f "${services["$service"]}" |
                            grep '^[0-9]\+' |
                            xargs -r ps --no-headers -o pid,command -p
                    )
                    flock $OUT_LOCK -c "echo "'"'"$RESULT"'"'"; echo"
                )&
            done
            wait
            rm $OUT_LOCK
        """ % {
            'bash_hash': ' '.join([
                '["%s"]="%s"' % (s, t,)
                for s, t in six.iteritems(service_to_target)
            ]),
        },
    )

    found_processes = {}
    for chunk in remote_output.split('\n\n'):
        lines = chunk.split('\n')
        service_name = lines[0]
        rest = lines[1:]

        found_processes[service_name] = [
            line.split(None, 1)
            for line in rest
        ]

    return found_processes


def kill_pids(
    host,
    pids,
    user,
    forward_ssh_agent,
    signal=DEFAULT_SIGNAL,
    kill_prefix=DEFAULT_KILL_PREFIX,
):
    if not pids:
        return

    return _check_output(
        _ssh_atoms(host, user, forward_ssh_agent) + ['bash'],
        stdin=' '.join(
            [kill_prefix, 'kill -s {0} {1} || true'],
        ).strip().format(
            signal,
            ' '.join(pids),
        ),
        stderr=None,
    )


def find_forgotten(host, services, user, forward_ssh_agent, signal=None, kill_prefix=''):
    service_names = []
    service_to_target = {}
    serivce_to_expecteds = {}
    for (
        service_name,
        globbable_pidfile,
        expected_pidfiles,
    ) in pidfiles_for_host(host, services):
        service_names.append(service_name)
        service_to_target[service_name] = globbable_pidfile
        serivce_to_expecteds[service_name] = expected_pidfiles

    lost_pids = {}
    instances = ssh_get_instances(
        host,
        service_to_target,
        user,
        forward_ssh_agent,
    )
    for service_name, pid_command_pairs in six.iteritems(instances):
        lost = [
            pid
            for pid, command in pid_command_pairs
            if not any(
                expected_pidfile in command
                for expected_pidfile in serivce_to_expecteds[service_name]
            )
        ]

        if lost:
            lost_pids[service_name] = lost

            if signal is not None:
                kill_pids(
                    host,
                    lost,
                    user,
                    forward_ssh_agent,
                    signal,
                    kill_prefix,
                )

    return lost_pids.items()


def _get_services_from_tron(tron_base):
    if not tron_base.endswith('/'):
        tron_base += '/'

    url = tron_base + 'api/services'

    return json.loads(request.urlopen(url).read())['services']


def main(command, tron_base, *target_service_names, **options):
    options.setdefault('signal', DEFAULT_SIGNAL)
    options.setdefault('kill_prefix', DEFAULT_KILL_PREFIX)
    options.setdefault('max_threads', DEFAULT_MAX_THREADS)
    options.setdefault('all_services', False)
    options.setdefault('forward_ssh_agent', False)
    options.setdefault('user', '')

    if options['all_services']:
        if target_service_names:
            raise Exception(
                "You passed in both --all-services and some service globs. "
                "That's confusing, and I'm refusing to operate. Use one or "
                "the other.",
            )

        def target_services_filterer(service): return True
    else:
        if not target_service_names:
            raise Exception(
                "You need to specify at least one expression to glob services "
                "or use the --all-services flag to confirm you want them all.",
            )

        def target_services_filterer(service): return any(
            fnmatch(service['name'], t)
            for t in target_service_names,
        )

    _print_event(contents={
        'event': 'get_services_from_tron_about_to_begin',
        'tron_base': tron_base,
    })
    services = _get_services_from_tron(tron_base)

    all_hosts = {
        node['hostname']
        for service in services
        for node in service['node_pool']['nodes']
    }

    target_services = [
        service
        for service in services
        if target_services_filterer(service)
    ]
    found_service_names = [s['name'] for s in target_services]
    _print_event(contents={
        'event': 'target_services_identified',
        'command': command,
        'globs': target_service_names,
        'targets': found_service_names,
    })

    pool = ThreadPool(min(len(all_hosts), options['max_threads']))

    results = list(
        pool.imap_unordered(
            lambda host: (
                host,
                find_forgotten(
                    host,
                    target_services,
                    options['user'],
                    options['forward_ssh_agent'],
                    options['signal'] if command == 'kill' else None,
                    options['kill_prefix'],
                ),
            ),
            all_hosts,
        ),
    )

    _print_event(contents={
        'event': 'discovery_result',
        'command': command,
        'targets': found_service_names,
        'data': results,
    })

    forgotten_count = sum(
        len(pids)
        for _, service_pid_pairs in results
        for _, pids in service_pid_pairs
    )
    if forgotten_count:
        _print_event(contents={
            'event': 'discovery_result_count',
            'command': command,
            'targets': found_service_names,
            'count': forgotten_count,
        })


def _make_opts():
    parser = optparse.OptionParser(
        usage='usage: %prog [options] <command> <tron_base> <target_service_name>*',
    )
    parser.add_option(
        '-a',
        '--all-services',
        action="store_true",
        dest="all_services",
        help="Consider every service.",
    )
    parser.add_option(
        '-j',
        '--max-threads',
        default=DEFAULT_MAX_THREADS,
        type=int,
        help="Max number of local threads to use to span ssh connections.",
    )

    ssh_group = optparse.OptionGroup(parser, 'ssh options')
    ssh_group.add_option(
        '-A',
        action="store_true",
        dest="forward_ssh_agent",
        help="Forward the ssh agent when connecting.",
    )
    ssh_group.add_option(
        '-u',
        '--user',
        default='',
        help="The user to use when ssh'ing into machines.",
    )
    parser.add_option_group(ssh_group)

    kill_group = optparse.OptionGroup(parser, 'kill options')
    kill_group.add_option(
        '-s',
        '--signal',
        default=DEFAULT_SIGNAL,
        help="The signal to send to lost processes. Default: %default",
    )
    kill_group.add_option(
        '--kill-prefix',
        default=DEFAULT_KILL_PREFIX,
        help="What to prepend to kill invocations. Useful for things like "
             "sudo'ing.",
    )
    parser.add_option_group(kill_group)

    options, positional = parser.parse_args()

    if len(positional) < 2:
        parser.error('I need both the command and the tron_base.')

    if positional[0] not in COMMANDS:
        parser.error("command must be one of " + ', '.join(COMMANDS))

    return (positional, options.__dict__)


if __name__ == "__main__":
    try:
        args, kwargs = _make_opts()
        main(*args, **kwargs)
    except BaseException as ex:
        _print_event(contents={
            'event': 'uncaught_exception',
            'ex_type': str(type(ex).__name__),
            'ex_message': str(ex),
            'ex_traceback': ''.join(traceback.format_tb(sys.exc_info()[-1])),
        })
