"""
Find processes missing from Tron.
"""
import json
import optparse
import subprocess
import urllib2
import sys
from fnmatch import fnmatch
from multiprocessing.pool import ThreadPool
from pprint import pprint
from tempfile import TemporaryFile



DEFAULT_SIGNAL = 'TERM'
DEFAULT_KILL_PREFIX = ''
DEFAULT_MAX_THREADS = 64


COMMANDS = (
    'list',
    'kill',
)


def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')

    if 'stdin' in kwargs:
        curr = kwargs['stdin']
        if isinstance(curr, basestring):
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
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
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


def ssh_get_instances(host, service_to_target):
    remote_output = _check_output(
        ['ssh', host, 'bash'],
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
                for s, t in service_to_target.iteritems()
            ])
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


def find_forgotten(host, services):
    sys.stdout.write('.')
    sys.stdout.flush()
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
    for service_name, pid_command_pairs in ssh_get_instances(
        host,
        service_to_target,
    ).iteritems():
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

    sys.stdout.write('\b \b')
    sys.stdout.flush()
    return lost_pids.items()


def kill_forgotten(
    host,
    results,
    signal=DEFAULT_SIGNAL,
    kill_prefix=DEFAULT_KILL_PREFIX,
):
    my_targets = sum((pids for _, pids in results[host]), [])

    if not my_targets:
        return

    return _check_output(
        ['ssh', '-A', host, 'bash'],
        stdin=' '.join(
            [kill_prefix, 'kill -s {0} {1} || true'],
        ).strip().format(
            signal,
            ' '.join(my_targets),
        ),
    )


def _get_services_from_tron(tron_base):
    if not tron_base.endswith('/'):
        tron_base += '/'

    url = tron_base + 'api/services'

    return json.loads(urllib2.urlopen(url).read())['services']


def main(command, tron_base, *target_service_names, **options):
    options.setdefault('signal', DEFAULT_SIGNAL)
    options.setdefault('kill_prefix', DEFAULT_KILL_PREFIX)
    options.setdefault('max_threads', DEFAULT_MAX_THREADS)

    print "Getting services"
    services = _get_services_from_tron(tron_base)
    print "done!"

    all_hosts = set([
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
    pool = ThreadPool(min(len(all_hosts), options['max_threads']))

    results = dict(
        pool.imap_unordered(
            lambda host: (host, find_forgotten(host, target_services),),
            all_hosts,
        ),
    )

    pprint(results)
    forgotten_count = sum(
        len(pids)
        for service_pid_pairs in results.itervalues()
        for _, pids in service_pid_pairs
    )
    print "Total instances", forgotten_count

    if forgotten_count and command == 'kill':
        for host, output in pool.imap_unordered(
            lambda host: (
                host,
                    kill_forgotten(
                    host,
                    results,
                    options['signal'],
                    options['kill_prefix'],
                ),
            ),
            set([
                hostname
                for hostname, pairs in results.iteritems()
                if pairs
            ]),
        ):
            if not output:
                continue
            print host, 'said: ', output

def _make_opts():
    parser = optparse.OptionParser(
        usage='usage: %prog [options] <command> <tron_base> <target_service_name>*'
    )
    parser.add_option(
        '-j',
        '--max-threads',
        default=DEFAULT_MAX_THREADS,
        type=int,
        help="Max number of local threads to use to span ssh connections.",
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
