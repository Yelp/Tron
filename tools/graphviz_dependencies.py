#!/usr/bin/env python
import shutil
import sys
import tempfile
import yaml

from tron import mcp

def clean_config(load):
    if hasattr(load, 'ssh_options'):
        del load.ssh_options

    return load

def read_config(contents):
    tmpdir = tempfile.mkdtemp()

    try:
        # The local MCP can't handle all the configuration, so clean it
        edited_config = clean_config(yaml.load(contents))

        edited_file = tempfile.NamedTemporaryFile()
        edited_file.write(yaml.dump(edited_config))
        edited_file.flush()

        master = mcp.MasterControlProgram(tmpdir, edited_file.name)
        master.load_config()
        return master
    except Exception, e:
        print >>sys.stderr, "Error in configuration: %s" % str(e)
    finally:
        shutil.rmtree(tmpdir)

    return None

if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] == '--help' or sys.argv[1] == '-h':
        print >> sys.stderr, "Generates a Graphviz-compatible representation of a job's dependencies"
        print >> sys.stderr, "Usage: %s <job_name>" % sys.argv[0]
        print >> sys.stderr, "Provide the tronfig.yaml file as stdin."
        print >> sys.stderr, "     Example: %s my_job < tronfig.yaml | dot -Tpdf -o my_job.pdf" % sys.argv[0]
        sys.exit(1)

    job_name = sys.argv[1]

    tron_config = sys.stdin.read()
    master = read_config(tron_config)

    job = master.jobs.get(job_name)
    if job is None:
        print >> sys.stderr, "%s could not be found in the config" % job_name
        sys.exit(1)

    print "digraph {"
    for each_node in job.topo_actions:
        if not each_node.required_actions:
            print "    node [shape = invhouse]; %s" % each_node.name
        else:
            print "    node [shape = rect]; %s" % each_node.name
        for each_required_action in each_node.required_actions:
            print "    %s -> %s" % (each_required_action.name, each_node.name)
    print "}"

