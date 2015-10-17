"""
Find processes missing from Tron.
"""
import json
import optparse
import sys
import urllib2
from fnmatch import fnmatch
from pprint import pprint

try:
    from fabric.version import VERSION, get_version
    if VERSION < (1, 3):
        raise Exception(
            "This script needs fabric>=1.3 installed. You have {0}".format(
                get_version()
            )
        )
    del VERSION, get_version
except ImportError:
    print >>sys.stderr, \
        "Can't import fabric. Please make sure it's in your sys.path"
    raise
except:
    raise Exception(
        "Can't detect fabric's version. This script needs fabric>=1.3. Make "
        "sure it's installed and in your sys.path"
    )

from fabric.api import run, settings, execute, parallel
from fabric.context_managers import hide
from fabric import state

state.env.forward_agent = True


DEFAULT_SIGNAL = 'TERM'
DEFAULT_KILL_PREFIX = ''


COMMANDS = (
    'list',
    'kill',
)


def clean_output(o, ignore_outputs_starting_with):
    if not ignore_outputs_starting_with:
        return o

    return '\n'.join([
        l.strip()
        for l in o.split('\n')
        if not l.startswith(ignore_outputs_starting_with)
    ])


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


def get_instances(search, ignore_outputs_starting_with=None):
    with settings(warn_only=True):
        with hide('stdout', 'stderr', 'warnings'):
            out = clean_output(
                run('pgrep -f %s' % search),
                ignore_outputs_starting_with,
            )
        pids = filter(
            bool,
            (
                l.strip()
                for l in out.replace('\r', '').split('\n')
            ),
        )
        if not pids:
            return []

        with hide('running', 'stdout', 'stderr', 'warnings'):
            out = clean_output(
                run('ps -o pid,command -p %s | cat' % ' '.join(pids)),
                ignore_outputs_starting_with,
            )
        result = [
            l.split(None, 1)
            for l in out.split('\n')[1:]
        ]
        return result
    return result

def find_forgotten(services, ignore_outputs_starting_with=None):
    with hide('running', 'stdout', 'stderr', 'warnings'):
        my_hostname = clean_output(
            run('hostname'),
            ignore_outputs_starting_with,
        ).strip()

    lost_pids = {}

    for (
        service_name,
        globbable_pidfile,
        expected_pidfiles,
    ) in pidfiles_for_host(my_hostname, services):
        unknowns = [
            pid
            for pid, command in get_instances(
                globbable_pidfile,
                ignore_outputs_starting_with,
            )
            if not any(pidfile in command for pidfile in expected_pidfiles)
        ]
        if unknowns:
            lost_pids[service_name] = unknowns

    return lost_pids.items()


def kill_forgotten(
    results,
    signal=DEFAULT_SIGNAL,
    kill_prefix=DEFAULT_KILL_PREFIX,
    ignore_outputs_starting_with=None,
):
    with hide('running', 'stdout', 'stderr', 'warnings'):
        my_hostname = clean_output(
            run('hostname'),
            ignore_outputs_starting_with,
        ).strip()

    my_targets = sum((pids for _, pids in results[my_hostname]), [])

    if not my_targets:
        return

    with settings(warn_only=True):
        clean_output(
            run(
                ' '.join([kill_prefix, 'kill -s {0} {1}']).strip().format(
                    signal,
                    ' '.join(my_targets),
                ),
            ),
            ignore_outputs_starting_with,
        )


def _get_services_from_tron(tron_base):
    if not tron_base.endswith('/'):
        tron_base += '/'

    url = tron_base + 'api/services'

    return json.loads(urllib2.urlopen(url).read())['services']


def main(command, tron_base, *target_service_names, **options):
    options.setdefault('signal', DEFAULT_SIGNAL)
    options.setdefault('kill_prefix', DEFAULT_KILL_PREFIX)
    options.setdefault('ignore_outputs_starting_with', None)

    services = _get_services_from_tron(tron_base)
    all_nodes = set([
        node['hostname']
        for service in services
        for node in service['node_pool']['nodes']
    ])

    if target_service_names:
        target_services = [
            service
            for service in services
            if any(fnmatch(service['name'], t) for t in target_service_names)
        ]
    else:
        target_services = services

    results = execute(
        parallel(find_forgotten),
        target_services,
        options['ignore_outputs_starting_with'],
        hosts=all_nodes,
    )

    pprint(results)
    forgotten_count = sum(
        len(pids)
        for service_pid_pairs in results.itervalues()
        for _, pids in service_pid_pairs
    )
    print "Total instances", forgotten_count

    if forgotten_count and command == 'kill':
        execute(
            parallel(kill_forgotten),
            results,
            options['signal'],
            options['kill_prefix'],
            options['ignore_outputs_starting_with'],
            hosts=set([
                hostname
                for hostname, pairs in results.iteritems()
                if pairs
            ]),
        )

def _make_opts():
    parser = optparse.OptionParser(
        usage='usage: %prog [options] <command> <tron_base> <target_service_name>*'
    )
    parser.add_option(
        '--ignore-outputs-starting-with',
        default=None,
        help="Ignore these lines in the stdout of the invoked program. This "
            "is useful when you have something that behaves funkily with "
            "fabirc in your profile/bashrc/etc.",
    )
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

    options, positional = parser.parse_args()

    if len(positional) < 2:
        parser.error('I need both the command and the tron_base.')

    if positional[0] not in COMMANDS:
        parser.error("command must be one of " + ', '.join(COMMANDS))

    return (positional, options.__dict__)


if __name__ == "__main__":
    args, kwargs = _make_opts()
    main(*args, **kwargs)
