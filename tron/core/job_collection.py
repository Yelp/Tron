import logging

import six
from six.moves import filter

from tron.core.job import Job
from tron.utils import collections
from tron.utils import iteration
from tron.utils import proxy

log = logging.getLogger(__name__)


class JobCollection:
    """A collection of jobs."""

    def __init__(self):
        self.jobs = collections.MappingCollection('jobs')
        self.proxy = proxy.CollectionProxy(
            lambda: six.itervalues(self.jobs),
            [
                proxy.func_proxy('enable', iteration.list_all),
                proxy.func_proxy('disable', iteration.list_all),
                proxy.func_proxy('schedule', iteration.list_all),
                proxy.func_proxy('run_queue_schedule', iteration.list_all),
            ],
        )

    def load_from_config(self, job_configs, factory, reconfigure):
        """Apply a configuration to this collection and return a generator of
        jobs which were added.
        """
        self.jobs.filter_by_name(job_configs)

        def map_to_job_and_schedule(job_schedulers):
            for job_scheduler in job_schedulers:
                if reconfigure:
                    job_scheduler.schedule()
                yield job_scheduler.get_job()

        seq = (factory.build(config) for config in six.itervalues(job_configs))
        return map_to_job_and_schedule(filter(self.add, seq))

    def add(self, job_scheduler):
        return self.jobs.add(job_scheduler, self.update)

    def move(self, old_name, new_name):
        job_scheduler = self.get_by_name(old_name)

        # check if job is running
        if job_scheduler.get_job().status == Job.STATUS_RUNNING:
            return f"Moving {old_name} to {new_name} failed. Job is still running."

        log.info(f"Moving {old_name} to {new_name}")
        job_scheduler.update_name(new_name)
        self.add(self.jobs.pop(old_name))

        return f"Moving {old_name} to {new_name} succeeded."

    def update(self, new_job_scheduler):
        log.info(f"Updating {new_job_scheduler}")
        job_scheduler = self.get_by_name(new_job_scheduler.get_name())
        job_scheduler.update_from_job_scheduler(new_job_scheduler)
        job_scheduler.schedule_reconfigured()
        return True

    def restore_state(self, job_state_data, config_action_runner):
        for name, state in job_state_data.items():
            self.jobs[name].restore_state(state, config_action_runner)
        log.info(f"Loaded state for {len(job_state_data)} jobs")

    def get_by_name(self, name):
        return self.jobs.get(name)

    def get_names(self):
        return self.jobs.keys()

    def get_jobs(self):
        return [sched.get_job() for sched in self]

    def get_job_run_collections(self):
        return [sched.get_job_runs() for sched in self]

    def __iter__(self):
        return six.itervalues(self.jobs)

    def __getattr__(self, name):
        return self.proxy.perform(name)

    def __contains__(self, name):
        return name in self.jobs
