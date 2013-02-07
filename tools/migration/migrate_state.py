"""
 Migrate a state file/database from one StateStore implementation to another.

 Usage:
    python tools/migration/migrate_state -s old_config_dir -d new_config_dir

 old_config.yaml and new_config.yaml should be configuration files with valid
 state_persistence sections. The state_persistence section configures the
 StateStore.

 Pre 0.5 state files can be read by the YamlStateStore. See the configuration
 documentation for more details on how to create state_persistence sections.
"""
from collections import namedtuple

import optparse
from tron.config import manager
from tron.serialize.runstate.statemanager import PersistenceManagerFactory

Item = namedtuple('Item', ['name', 'state_data'])


def parse_options():
    parser = optparse.OptionParser()
    parser.add_option('-s', '--source',
        help="The source configuration path which contains a state_persistence "
             "section configured for the state file/database.")
    parser.add_option('-d', '--dest',
        help="The destination configuration path which contains a "
             "state_persistence section configured for the state file/database.")

    opts, args = parser.parse_args()

    if not opts.source:
        parser.error("--source is required")
    if not opts.dest:
        parser.error("--dest is required.")

    return opts, args


def get_state_manager_from_config(config_path):
    """Return a state manager that is configured in the file at
    config_filename.
    """
    config_manager = manager.ConfigManager(config_path)
    config_container = config_manager.load()
    state_config = config_container.get_master().state_persistence
    return PersistenceManagerFactory.from_config(state_config)


def get_current_master_config(config_path):
    config_manager = manager.ConfigManager(config_path)
    config_container = config_manager.load()
    return config_container.get_master()


def convert_state(opts):
    source_manager  = get_state_manager_from_config(opts.source)
    dest_manager    = get_state_manager_from_config(opts.dest)
    config          = get_current_master_config(opts.source)

    msg = "Migrating state from %s to %s"
    print msg % (source_manager._impl, dest_manager._impl)

    job_items, service_items = config.jobs.values(), config.services.values()
    jobs_states, services_states = source_manager.restore(job_items, service_items)
    source_manager.cleanup()

    for name, job in jobs_states.iteritems():
        dest_manager.save_job(Item(name, job))
    print "Migrated %s jobs." % len(jobs_states)

    for name, service in services_states.iteritems():
        dest_manager.save_service(Item(name, service))
    print "Migrated %s services." % len(services_states)

    dest_manager.cleanup()


if __name__ == "__main__":
    opts, _args = parse_options()
    convert_state(opts)