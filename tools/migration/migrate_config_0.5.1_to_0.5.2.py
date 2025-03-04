"""Migrate a single configuration file (tron 0.5.1) to the new 0.5.2
multi-file format.

Usage:

python tools/migration/migrate_config_0.5.1_to_0.5.2.py \
    --source old_config_filename \
    --dest new_config_dir
"""

import optparse
import os

from tron.config import manager


def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("-s", "--source", help="Path to old configuration file.")
    parser.add_option(
        "-d",
        "--dest",
        help="Path to new configuration directory.",
    )
    opts, _ = parser.parse_args()

    if not opts.source:
        parser.error("--source is required")
    if not opts.dest:
        parser.error("--dest is required")
    return opts


def main(source, dest):
    dest = os.path.abspath(dest)
    if not os.path.isfile(source):
        raise SystemExit("Error: Source (%s) is not a file" % source)
    if os.path.exists(dest):
        raise SystemExit("Error: Destination path (%s) already exists" % dest)
    old_config = manager.read_raw(source)
    manager.create_new_config(dest, old_config)


if __name__ == "__main__":
    opts = parse_options()
    main(opts.source, opts.dest)
