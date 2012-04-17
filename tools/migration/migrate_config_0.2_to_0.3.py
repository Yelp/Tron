"""
 Convert a 0.2.x Tron configuration file to the 0.3 format.

 Removes YAML anchors, references, and tags.

 Does not move node pools out of 'nodes' section.
 Does not enforce a list for action requires.

"""
import optparse
import re
import sys
import yaml

from tron.config import config_parse

class Loader(yaml.Loader):
    """A YAML loader that does not clear its anchor mapping."""

    def compose_document(self):
        self.get_event()
        node = self.compose_node(None, None)
        self.get_event()
        return node


def strip_tags(source):
    """Remove YAML tags."""
    return config_parse.YAML_TAG_RE.sub('', source)


def name_from_doc(doc):
    """Find the string idenfitier for a doc."""
    if 'name' in doc:
        return doc['name']

    # Special case for node without a name, their name defaults to their hostname
    if set(doc.keys()) == set(['hostname']):
        return doc['hostname']

    if set(doc.keys()) == set(['nodes']):
        raise ValueError("Please create a name for NodePool %s" % doc)

    raise ValueError("Could not find a name for %s" % doc)


def warn_node_pools(content):
    """Update references for node pools and split nodes from node pools."""
    node_pools = []
    doc = yaml.safe_load(content)

    for node_doc in doc['nodes']:
        if 'nodes' not in node_doc:
            continue
        node_pools.append(node_doc)

    if not node_pools:
        return

    print >>sys.stderr, ("\n\nNode Pools should be moved into a node_pools section." +
        " The following node pools were found:\n" +
        "\n".join(str(n) for n in node_pools))


def warn_requires_list(content):
    action_names = []
    doc = yaml.safe_load(content)

    for job in doc['jobs']:
        for action in job['actions']:
            if 'requires' not in action:
                continue

            if isinstance(action['requires'], list):
                continue

            action_names.append("%s.%s" % (job['name'], action['name']))

    if not action_names:
        return

    print >>sys.stderr, ("\n\nAction requires should be a list." +
        " The following actions have requires that are not lists:\n" +
        "\n".join(action_names))


def create_loader(content):
    """Create a loader, and have it create the document from content."""
    loader = Loader(content)
    try:
        loader.get_single_node()
    finally:
        loader.dispose()
    return loader


def build_anchor_mapping(content):
    """Return a map of anchors to the new name to use."""
    loader = create_loader(content)

    mapping = {}
    for anchor_name, yaml_node in loader.anchors.iteritems():
        doc = loader.construct_document(yaml_node)
        mapping[anchor_name] = name_from_doc(doc)

    return mapping


def update_references(content):
    anchor_mapping = build_anchor_mapping(content)

    key_length_func = lambda (k, v): len(k)
    anchors_by_length = sorted(
            anchor_mapping.iteritems(), key=key_length_func, reverse=True)
    for anchor_name, string_name in anchors_by_length:
        # Remove the anchors
        content = re.sub(r'\s*&%s ?' % anchor_name, '', content)
        # Update the reference to use the string identifier
        content = re.sub(r'\*%s\b' % anchor_name, string_name, content)

    return content


def convert(source, dest):
    with open(source, 'r') as fh:
        content = fh.read()

    try:
        content = strip_tags(content)
        content = update_references(content)
        warn_node_pools(content)
        warn_requires_list(content)
    except yaml.scanner.ScannerError, e:
        print "Bad content: %s\n%s" % (e, content)

    with open(dest, 'w') as fh:
        fh.write(content)


if __name__ == "__main__":
    opt_parser = optparse.OptionParser()
    opt_parser.add_option('-s', dest="source", help="Source config filename.")
    opt_parser.add_option('-d', dest="dest", help="Destination filename.")
    opts, args = opt_parser.parse_args()
    if not opts.source or not opts.dest:
        print >>sys.stderr, "Source and destination filenames required."
        sys.exit(1)

    convert(opts.source, opts.dest)