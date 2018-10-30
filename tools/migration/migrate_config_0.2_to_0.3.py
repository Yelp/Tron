"""
 Convert a 0.2.x Tron configuration file to the 0.3 format.

 Removes YAML anchors, references, and tags.
 Display warnings for NodePools under the nodes section.
 Display warnings for action requires sections that are not lists.

"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import optparse
import re
import sys

import six
import yaml

YAML_TAG_RE = re.compile(r'!\w+\b')


class Loader(yaml.Loader):
    """A YAML loader that does not clear its anchor mapping."""

    def compose_document(self):
        self.get_event()
        node = self.compose_node(None, None)
        self.get_event()
        return node


def strip_tags(source):
    """Remove YAML tags."""
    return YAML_TAG_RE.sub('', source)


def name_from_doc(doc):
    """Find the string identifier for a doc."""
    if 'name' in doc:
        return doc['name']

    # Special case for node without a name, their name defaults to their hostname
    if set(doc.keys()) == {'hostname'}:
        return doc['hostname']

    if set(doc.keys()) == {'nodes'}:
        raise ValueError("Please create a name for NodePool %s" % doc)

    raise ValueError("Could not find a name for %s" % doc)


def warn_node_pools(content):
    doc = yaml.safe_load(content)

    node_pools = [node_doc for node_doc in doc['nodes'] if 'nodes' in node_doc]

    if not node_pools:
        return

    print(
        "\n\nNode Pools should be moved into a node_pools section." +
        " The following node pools were found:\n" +
        "\n".join(str(n) for n in node_pools),
        file=sys.stderr,
    )


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

    print(
        "\n\nAction requires should be a list." +
        " The following actions have requires that are not lists:\n" +
        "\n".join(action_names),
        file=sys.stderr,
    )


def create_loader(content):
    """Create a loader, and have it create the document from content."""
    loader = Loader(content)
    loader.get_single_node()
    return loader


def build_anchor_mapping(content):
    """Return a map of anchors to the new name to use."""
    loader = create_loader(content)

    return {
        anchor_name: name_from_doc(loader.construct_document(yaml_node))
        for anchor_name, yaml_node in six.iteritems(loader.anchors)
    }


def update_references(content):
    anchor_mapping = build_anchor_mapping(content)

    def key_length_func(kv):
        return len(kv[0])

    anchors_by_length = sorted(
        six.iteritems(anchor_mapping),
        key=key_length_func,
        reverse=True,
    )
    for anchor_name, string_name in anchors_by_length:
        # Remove the anchors
        content = re.sub(r'\s*&%s ?' % anchor_name, '', content)
        # Update the reference to use the string identifier
        content = re.sub(
            r'\*%s\b' % anchor_name,
            '"%s"' % string_name,
            content,
        )

    return content


def convert(source, dest):
    with open(source, 'r') as fh:
        content = fh.read()

    try:
        content = strip_tags(content)
        content = update_references(content)
        warn_node_pools(content)
        warn_requires_list(content)
    except yaml.scanner.ScannerError as e:
        print("Bad content: %s\n%s" % (e, content))

    with open(dest, 'w') as fh:
        fh.write(content)


if __name__ == "__main__":
    opt_parser = optparse.OptionParser()
    opt_parser.add_option('-s', dest="source", help="Source config filename.")
    opt_parser.add_option('-d', dest="dest", help="Destination filename.")
    opts, args = opt_parser.parse_args()
    if not opts.source or not opts.dest:
        print("Source and destination filenames required.", file=sys.stderr)
        sys.exit(1)

    convert(opts.source, opts.dest)
