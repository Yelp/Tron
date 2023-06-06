import argparse
import logging

from tron.config import manager
from tron.serialize import runstate
from tron.serialize.runstate.statemanager import PersistenceManagerFactory
from tron.utils import chdir


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--back",
        help="Flag to migrate back from new state back to old state",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--working-dir",
        help="Working directory for the Tron daemon",
        required=True,
    )
    parser.add_argument(
        "--config-path",
        help="Path in working dir with configs",
        required=True,
    )
    return parser.parse_args()


def create_job_runs_for_job(state_manager, job_name, job_state):
    for run in job_state["runs"]:
        run_num = run["run_num"]
        state_manager.save(runstate.JOB_RUN_STATE, f"{job_name}.{run_num}", run)
    run_nums = [run["run_num"] for run in job_state["runs"]]
    job_state["run_nums"] = run_nums
    # Note: not removing 'runs' from job_state for safety.
    # If Tron starts up correctly after the state migration, it will update the job state
    # and remove 'runs'.
    state_manager.save(runstate.JOB_STATE, job_name, job_state)


def move_job_runs_to_job(state_manager, job_name, job_state):
    runs = state_manager._restore_runs_for_job(job_name, job_state)
    job_state["runs"] = runs
    state_manager.save(runstate.JOB_STATE, job_name, job_state)
    for run in runs:
        state_manager.delete(runstate.JOB_RUN_STATE, f'{job_name}.{run["run_num"]}')


def update_state(state_manager, job_names, back):
    jobs = state_manager._restore_dicts(runstate.JOB_STATE, job_names)
    for job_name, job_state in jobs.items():
        if back:
            move_job_runs_to_job(state_manager, job_name, job_state)
        else:
            create_job_runs_for_job(state_manager, job_name, job_state)


def migrate_state(config_path, working_dir, back):
    with chdir(working_dir):
        config_manager = manager.ConfigManager(config_path)
        config_container = config_manager.load()
    job_names = config_container.get_job_names()
    state_config = config_container.get_master().state_persistence
    state_manager = PersistenceManagerFactory.from_config(state_config)
    update_state(state_manager, job_names, back)
    state_manager.cleanup()


if __name__ == "__main__":
    # INFO for boto, DEBUG for all tron-related state logs
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("tron").setLevel(logging.DEBUG)

    args = parse_args()
    migrate_state(args.config_path, args.working_dir, args.back)
