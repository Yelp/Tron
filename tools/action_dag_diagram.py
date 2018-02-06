"""
 Create a graphviz diagram from a Tron Job configuration.

 Usage:
    python tools/action_dag_diagram.py -c <config> -n <job_name>

 This will create a file named <job_name>.dot
 You can create a diagram using:
    dot -Tpng -o <job_name>.png <job_name>.dot
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import optparse

from tron.config import manager
from tron.config import schema


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', help="Tron configuration path.")
    parser.add_option(
        '-n', '--name',
        help="Job name to graph. Also used as output filename.",
    )
    parser.add_option(
        '--namespace', default=schema.MASTER_NAMESPACE,
        help="Configuration namespace which contains the job.",
    )
    opts, _ = parser.parse_args()

    if not opts.config:
        parser.error("A config filename is required.")
    if not opts.name:
        parser.error("A Job name is required.")
    return opts


def build_diagram(job_config):
    edges, nodes = [], []

    for action in job_config.actions.itervalues():
        shape = 'invhouse' if not action.requires else 'rect'
        nodes.append("node [shape = %s]; %s" % (shape, action.name))
        for required_action in action.requires:
            edges.append("%s -> %s" % (required_action, action.name))

    return "digraph g{%s\n%s}" % ('\n'.join(nodes), '\n'.join(edges))


def get_job(config_container, namespace, job_name):
    if namespace not in config_container:
        raise ValueError("Unknown namespace: %s" % namespace)

    config = config_container[opts.namespace]
    if job_name not in config.jobs:
        raise ValueError("Could not find Job %s" % job_name)

    return config.jobs[job_name]


if __name__ == '__main__':
    opts = parse_args()

    config_manager = manager.ConfigManager(opts.config)
    container = config_manager.load()
    job_config = get_job(container, opts.namespace, opts.name)
    graph = build_diagram(job_config)

    with open('%s.dot' % opts.name, 'w') as fh:
        fh.write(graph)
