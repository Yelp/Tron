#!/usr/bin/env python3.6
#
# get_tron_metrics.py
#   This script is designed to retrieve metrics from Tron via its API and send
#   send them to meteorite.
import logging
import pprint
import subprocess
import sys
import textwrap

from tron.commands import cmd_utils
from tron.commands.client import Client

log = logging.getLogger('get_tron_metrics')


def parse_cli():
    parser = cmd_utils.build_option_parser()
    parser.description = (
        "Collects metrics from Tron via its API and forwards them to "
        "meteorite."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Don't actually send metrics out. Defaults: %(default)s"
    )
    parser.add_argument(
        "--cluster",
        default=None,
        type=str,
        help=(
            "Cluster from which these metrics originate. "
            "Sent as a dimension to meteorite. "
            "Default: %(default)s"
        ),
    )
    args = parser.parse_args()
    return args


def check_bin_exists(bin):
    """
    Checks if an executable binary exists

    :param bin: (str) Name of the executable; could be a path to one
    """
    return subprocess.call(
        ['which', bin],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) == 0


def send_data_metric(name, metric_type, value, dimensions={}, dry_run=False):
    """
    Sends a single data point to meteorite via bash command

    :param name: (str) Name of the metric
    :param metric_type: (str) Type of the meteorite metric. Must be in
        METEORITE_TYPES
    :param value: (float) Value of the metric
    :param dimensions: (dict) Metric dimensions as key-value pairs
    :param dry_run: (bool) Whether or not to send metrics to meteorite
    """
    if dry_run:
        metric_args = dict(
            name=name,
            metric_type=metric_type,
            value=value,
            dimensions=dimensions,
        )
        log.info(
            f"Would have sent this to meteorite:\n"
            f"{pprint.pformat(metric_args)}"
        )
        return

    cmd = ['meteorite', 'data', '-v', name, metric_type, str(value)]
    for k, v in dimensions.items():
        cmd.extend(['-d', f"{k}:{v}"])

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output, error = process.communicate()
    output = output.decode('utf-8').rstrip()
    error = error.decode('utf-8').rstrip()

    if process.returncode != 0:
        log.error(
            "Meteorite failed with:\n"
            f"{textwrap.indent(error, '    ')}"
        )
    else:
        log.debug(f"From meteorite: {output}")


def send_counter(name, **kwargs):
    send_data_metric(
        name=name,
        metric_type='counter',
        value=kwargs.pop('count'),
        dimensions=kwargs.pop('dimensions', {}),
        dry_run=kwargs.pop('dry_run', False),
    )


def send_gauge(name, **kwargs):
    send_data_metric(
        name=name,
        metric_type='gauge',
        value=kwargs.pop('value'),
        dimensions=kwargs.pop('dimensions', {}),
        dry_run=kwargs.pop('dry_run', False),
    )


def send_meter(name, **kwargs):
    send_counter(name, **kwargs)  # We ignore mX_rate args


def send_histogram(name, **kwargs):
    for k in ['p50', 'p75', 'p95', 'p99']:  # Only send p50-99
        gauge_name = f"{name}.{k}"
        kwargs['value'] = kwargs[k]  # set for gauge
        send_gauge(gauge_name, **kwargs)


def send_timer(name, **kwargs):
    # We mirror the metrics implementation in Tron by splitting timer into a
    # meter and a histogram
    send_meter(name, **kwargs)
    send_histogram(name, **kwargs)


_METRIC_SENDERS = {
    'counter': send_counter,
    'gauge': send_gauge,
    'meter': send_meter,
    'histogram': send_histogram,
    'timer': send_timer,
}


def send_metrics(metrics, cluster=None, dry_run=False):
    """
    Send metrics via meteorite

    :param metrics: Dictionary of metrics types and their data
    """
    for metric_type, data in metrics.items():
        for kwargs in data:
            name = kwargs.pop('name')
            kwargs['dry_run'] = dry_run

            if cluster:
                dimensions = kwargs.get('dimensions', {})
                dimensions['tron_cluster'] = cluster
                kwargs['dimensions'] = dimensions

            _METRIC_SENDERS[metric_type](name, **kwargs)


def main():
    args = parse_cli()
    cmd_utils.setup_logging(args)
    cmd_utils.load_config(args)
    client = Client(args.server)

    if check_bin_exists('meteorite'):
        metrics = client.metrics()
        send_metrics(metrics, cluster=args.cluster, dry_run=args.dry_run)
    else:
        log.error("'meteorite' was not found")


if __name__ == '__main__':
    sys.exit(main())
