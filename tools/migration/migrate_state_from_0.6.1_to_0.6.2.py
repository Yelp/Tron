"""Usage: %prog [options] <working dir> <new filename>

This is a script to convert old state storing containers into the new
objects used by Tron v0.6.2 and tronstore. The script will use the same
mechanism for storing state as specified in the Tron configuration file.
Config elements can be overriden via command line options, which allows for
full configuration of the mechanism used to store the new state object.

Please ensure that you have Tron v0.6.2 before running this script. Also note
that migrate_state.py will NOT work again until running this script, as it has
been changed to work with v0.6.2's method of state storing.

The working dir should generally be the same as the one used when launching
trond, but should contain the file pointed to by the configuration file.
The script attempts to load a configuration from <working dir>/config by
default, or whatever -f was set to.

***IMPORTANT***
When using SQLAlchemy/MongoDB storing mechanisms, the -c option for setting
connection detail parameters MUST be set.

HOWEVER, THE SCRIPT DOES NOT CHECK WHETHER OR NOT THE CONNECTION DETAILS
ARE THE SAME, NOR IF YOU ARE GOING TO CLOBBER YOUR OLD DATABASE WITH THE GIVEN
CONNECTION AND CONFIGURATION PARAMETERS.

Please especially ensure that you are not connecting to the exact
same SQL database that holds your old state_data, or you are likely to run
into a large number of strange problems and inconsistencies.
***IMPORTANT***


Command line options:
    -c str   Set new connection details to str for SQL/MongoDB storing. This
             is REQUIRED for using SQL/MongoDB as the new state store.

    -m str   Set a new mechanism for storing the new state objects.
             Defaults to whatever store_type was set to in the Tron
             configuration file.
             Options for str are sql, mongo, yaml, and shelve.

    -d str   Set a new method for storing state data within an SQL database.
             Defaults to whatever was set to db_store_method in the Tron
             configuration file, or json if it isn't set. Only used if
             SQLAlchemy is the storing mechanism.
             Options for str are pickle, yaml, msgpack, and json.

    -f str   Set the path for the configuration dir to str. This defaults to
             <working_dir>/config
"""

import sys
import os
import copy
import logging

from tron.commands import cmd_utils
from tron.config import ConfigError
from tron.config.schema import StatePersistenceTypes
from tron.config.manager import ConfigManager
from tron.serialize import runstate
from tron.serialize.runstate.shelvestore import ShelveStateStore
from tron.serialize.runstate.mongostore import MongoStateStore
from tron.serialize.runstate.yamlstore import YamlStateStore
from tron.serialize.runstate.sqlalchemystore import SQLAlchemyStateStore
from tron.serialize.runstate.tronstore.parallelstore import ParallelStore
from tron.serialize.runstate.statemanager import StateMetadata
from tron.serialize.runstate.tronstore import serialize

def parse_options():
    usage = "usage: %prog [options] <working dir> <new filename>"
    parser = cmd_utils.build_option_parser(usage)
    parser.add_option("-c", type="string",
                      help="Set new connection details for db connections",
                      dest="new_connection_details", default=None)
    parser.add_option("-m", type="string",
                      help="Set new state storing mechanism (store_type)",
                      dest="store_type", default=None)
    parser.add_option("-d", type="string",
                      help="Set new SQL db serialization method (db_store_method)",
                      dest="db_store_method", default=None)
    parser.add_option("-f", type="string",
                      help="Set path to Tron configuration file",
                      dest="conf_dir", default=None)
    options, args = parser.parse_args(sys.argv)
    return options, args[1], args[2]

def parse_config(conf_dir):
    if conf_dir:
        manager = ConfigManager(conf_dir)
    else:
        manager = ConfigManager('config')
    return manager.load()

def get_old_state_store(state_info):
    name = state_info.name
    connection_details = state_info.connection_details
    store_type = state_info.store_type

    if store_type == StatePersistenceTypes.shelve:
        return ShelveStateStore(name)

    if store_type == StatePersistenceTypes.sql:
        return SQLAlchemyStateStore(name, connection_details)

    if store_type == StatePersistenceTypes.mongo:
        return MongoStateStore(name, connection_details)

    if store_type == StatePersistenceTypes.yaml:
        return YamlStateStore(name)

def compile_new_info(options, state_info, new_file):
    new_state_info = copy.deepcopy(state_info)

    new_state_info = new_state_info._replace(name=new_file)

    if options.store_type:
        new_state_info = new_state_info._replace(store_type=options.store_type)

    if options.db_store_method:
        new_state_info = new_state_info._replace(db_store_method=options.db_store_method)

    if options.new_connection_details:
        new_state_info = new_state_info._replace(connection_details=options.new_connection_details)
    elif new_state_info.store_type in ('sql', 'mongo'):
        raise ConfigError('Must specify connection_details using -c to use %s'
            % new_state_info.store_type)

    return new_state_info

def assert_copied(new_store, data, key):
    """A small function to counter race conditions. It's possible that
    tronstore will serve the restore request BEFORE the save request, which
    will result in an Exception. We simply retry 10 times (which should be more
    than enough time for tronstore to serve the save request)."""

    if new_store.process.config.store_type == 'mongo':
        data['_id'] = key.key
    for i in range(10):
        try:
            new_data = new_store.restore([key])[key]
        except:
            continue

        if data == new_data:
            return

        method = new_store.process.config.db_store_method
        if method:
            try:
                serial_class = serialize.serialize_class_map[method]
                if serial_class.deserialize(serial_class.serialize(data)) == new_data:
                    return
            except:
                continue

    raise AssertionError('The value %s failed to copy.' % key.iden)

def copy_metadata(old_store, new_store):
    meta_key_old = old_store.build_key(runstate.MCP_STATE, StateMetadata.name)
    old_metadata_dict = old_store.restore([meta_key_old])
    if old_metadata_dict:
        old_metadata = old_metadata_dict[meta_key_old]
        if 'version' in old_metadata:
            old_metadata['version'] = (0, 6, 2, 0)
        meta_key_new = new_store.build_key(runstate.MCP_STATE, StateMetadata.name)
        new_store.save([(meta_key_new, old_metadata)])
        assert_copied(new_store, old_metadata, meta_key_new)

def copy_services(old_store, new_store, service_names):
    for service in service_names:
        service_key_old = old_store.build_key(runstate.SERVICE_STATE, service)
        old_service_dict = old_store.restore([service_key_old])
        if old_service_dict:
            old_service_data = old_service_dict[service_key_old]
            service_key_new = new_store.build_key(runstate.SERVICE_STATE, service)
            new_store.save([(service_key_new, old_service_data)])
            assert_copied(new_store, old_service_data, service_key_new)

def copy_jobs(old_store, new_store, job_names):
    for job in job_names:
        job_key_old = old_store.build_key(runstate.JOB_STATE, job)
        old_job_dict = old_store.restore([job_key_old])
        if old_job_dict:
            old_job_data = old_job_dict[job_key_old]
            job_state_key = new_store.build_key(runstate.JOB_STATE, job)

            run_ids = []
            for job_run in old_job_data['runs']:
                run_ids.append(job_run['run_num'])
                job_run_key = new_store.build_key(runstate.JOB_RUN_STATE,
                    job + ('.%s' % job_run['run_num']))
                new_store.save([(job_run_key, job_run)])
                assert_copied(new_store, job_run, job_run_key)

            run_ids = sorted(run_ids, reverse=True)
            job_state_data = {'enabled': old_job_data['enabled'], 'run_ids': run_ids}
            new_store.save([(job_state_key, job_state_data)])
            assert_copied(new_store, job_state_data, job_state_key)


def main():
    logging.basicConfig(level=logging.ERROR)
    print('Parsing options...')
    (options, working_dir, new_fname) = parse_options()
    os.chdir(working_dir)
    print('Parsing configuration file...')
    config = parse_config(options.conf_dir)
    state_info = config.get_master().state_persistence
    print('Setting up the old state storing object...')
    old_store = get_old_state_store(state_info)
    print('Setting up the new state storing object...')
    new_state_info = compile_new_info(options, state_info, new_fname)
    new_store = ParallelStore()
    if not new_store.load_config(new_state_info):
        raise AssertionError("Invalid configuration.")

    print('Copying metadata...')
    copy_metadata(old_store, new_store)
    print('Copying service data...')
    copy_services(old_store, new_store, config.get_services().keys())
    print('Converting job data...')
    copy_jobs(old_store, new_store, config.get_jobs().keys())
    print('Done copying. All data has been verified.')
    print('Cleaning up, just a sec...')
    old_store.cleanup()
    new_store.cleanup()

if __name__ == "__main__":
    main()
