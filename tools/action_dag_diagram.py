"""
 Create a graphviz diagram from a Tron Job configuration.

 Usage:
    python tools/action_dag_diagram.py -c <config> -n <job_name>

 This will create a file named <job_name>.dot
 You can create a diagram using:
    dot -Tpng -o <job_name>.png <job_name>.dot
"""
import optparse
from tron.config import config_parse

def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', help="Tron configuration filename.")
    parser.add_option('-n', '--name',
            help="Job name to graph. Also used as output filename.")
    opts, _ = parser.parse_args()

    if not opts.config:
        parser.error("A config filename is required.")
    if not opts.name:
        parser.error("A Job name is required.")
    return opts


def build_diagram(config, job_name):
    if job_name not in config.jobs:
        raise ValueError("Could not find Job %s" % job_name)

    job             = config.jobs[job_name]
    edges, nodes    = [], []

    for action in job.actions.itervalues():
        shape = 'invhouse' if not action.requires else 'rect'
        nodes.append("node [shape = %s]; %s" % (shape, action.name))
        for required_action in action.requires:
            edges.append("%s -> %s" % (required_action, action.name))

    return "digraph g{%s\n%s}" % ('\n'.join(nodes), '\n'.join(edges))


if __name__ == '__main__':
    opts = parse_args()

    with open(opts.config, 'r') as fh:
        config = config_parse.load_config(fh)
    graph = build_diagram(config, opts.name)

    with open('%s.dot' % opts.name, 'w') as fh:
        fh.write(graph)
