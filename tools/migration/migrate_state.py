"""
 Migrate a state file/database from one StateStore implementation to another. It
 may also be used to add namespace names to jobs/services when upgrading
 from pre-0.5.2 to version 0.5.2.

 Usage:
    python tools/migration/migrate_state.py \
        -s <old_config_dir> -d <new_config_dir> [ --namespace ]

 old_config.yaml and new_config.yaml should be configuration files with valid
 state_persistence sections. The state_persistence section configures the
 StateStore.

 Pre 0.5 state files can be read by the YamlStateStore. See the configuration
 documentation for more details on how to create state_persistence sections.
"""
from collections import namedtuple

import optparse
from tron.config import manager, schema
from tron.serialize.runstate.statemanager import PersistenceManagerFactory
from tron.utils import tool_utils

Item = namedtuple('Item', ['name', 'state_data'])


def parse_options():
    parser = optparse.OptionParser()
    parser.add_option('-s', '--source',
        help="The source configuration path which contains a state_persistence "
             "section configured for the state file/database.")
    parser.add_option('-d', '--dest',
        help="The destination configuration path which contains a "
             "state_persistence section configured for the state file/database.")
    parser.add_option('--source-working-dir',
        help="The working directory for source dir to resolve relative paths.")
    parser.add_option('--dest-working-dir',
        help="The working directory for dest dir to resolve relative paths.")
    parser.add_option('--namespace', action='store_true',
        help="Move jobs/services which are missing a namespace to the MASTER")

    opts, args = parser.parse_args()

    if not opts.source:
        parser.error("--source is required")
    if not opts.dest:
        parser.error("--dest is required.")

    return opts, args


def get_state_manager_from_config(config_path, working_dir):
    """Return a state manager from the configuration.
    """
    config_manager = manager.ConfigManager(config_path)
    config_container = config_manager.load()
    state_config = config_container.get_master().state_persistence
    with tool_utils.working_dir(working_dir):
        return PersistenceManagerFactory.from_config(state_config)


def get_current_config(config_path):
    config_manager = manager.ConfigManager(config_path)
    return config_manager.load()


def add_namespaces(state_data):
    return dict(('%s.%s' % (schema.MASTER_NAMESPACE, name), data)
                for (name, data) in state_data.iteritems())

def strip_namespace(names):
    return [name.split('.', 1)[1] for name in names]


def convert_state(opts):
    source_manager  = get_state_manager_from_config(opts.source, opts.source_working_dir)
    dest_manager    = get_state_manager_from_config(opts.dest, opts.dest_working_dir)
    container       = get_current_config(opts.source)

    msg = "Migrating state from %s to %s"
    print msg % (source_manager._impl, dest_manager._impl)

    job_names, service_names = container.get_job_and_service_names()
    if opts.namespace:
        job_names       = strip_namespace(job_names)
        service_names   = strip_namespace(service_names)

    job_states, service_states = source_manager.restore(job_names, service_names)
    source_manager.cleanup()

    if opts.namespace:
        job_states      = add_namespaces(job_states)
        service_states  = add_namespaces(service_states)

    for name, job in job_states.iteritems():
        dest_manager.save_job(Item(name, job))
    print "Migrated %s jobs." % len(job_states)

    for name, service in service_states.iteritems():
        dest_manager.save_service(Item(name, service))
    print "Migrated %s services." % len(service_states)

    dest_manager.cleanup()


if __name__ == "__main__":
    opts, _args = parse_options()
    convert_state(opts)