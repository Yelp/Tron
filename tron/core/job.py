import logging

import humanize
import six
from six.moves import filter

from tron import command_context
from tron import eventloop
from tron import node
from tron.core import actiongraph
from tron.core import jobrun
from tron.core import recovery
from tron.core.actionrun import ActionRun
from tron.scheduler import scheduler_from_config
from tron.serialize import filehandler
from tron.utils import collections
from tron.utils import iteration
from tron.utils import maybe_decode
from tron.utils import proxy
from tron.utils import timeutils
from tron.utils.observer import Observable
from tron.utils.observer import Observer


class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger(__name__)


class Job(Observable, Observer):
    """A configurable data object.

    Job uses JobRunCollection to manage its runs, and ActionGraph to manage its
    actions and their dependency graph.
    """

    STATUS_DISABLED = "disabled"
    STATUS_ENABLED = "enabled"
    STATUS_UNKNOWN = "unknown"
    STATUS_RUNNING = "running"

    NOTIFY_STATE_CHANGE = 'notify_state_change'
    NOTIFY_RUN_DONE = 'notify_run_done'

    context_class = command_context.JobContext

    # These attributes determine equality between two Job objects
    equality_attributes = [
        'name',
        'queueing',
        'scheduler',
        'node_pool',
        'all_nodes',
        'action_graph',
        'output_path',
        'action_runner',
        'max_runtime',
        'allow_overlap',
        'monitoring',
        'time_zone',
        'expected_runtime',
    ]

    # TODO: use config object
    def __init__(
        self,
        name,
        scheduler,
        eventbus_publish,
        queueing=True,
        all_nodes=False,
        monitoring=None,
        node_pool=None,
        enabled=True,
        action_graph=None,
        run_collection=None,
        parent_context=None,
        output_path=None,
        allow_overlap=None,
        action_runner=None,
        max_runtime=None,
        time_zone=None,
        expected_runtime=None
    ):
        super(Job, self).__init__()
        self.name = maybe_decode(name)
        self.monitoring = monitoring
        self.action_graph = action_graph
        self.scheduler = scheduler
        self.eventbus_publish = eventbus_publish
        self.runs = run_collection
        self.queueing = queueing
        self.all_nodes = all_nodes
        self.enabled = enabled
        self.node_pool = node_pool
        self.allow_overlap = allow_overlap
        self.action_runner = action_runner
        self.max_runtime = max_runtime
        self.time_zone = time_zone
        self.expected_runtime = expected_runtime
        self.output_path = output_path or filehandler.OutputPath()
        self.output_path.append(name)
        self.context = command_context.build_context(self, parent_context)
        log.info(f'{self} created')

    @classmethod
    def from_config(
        cls,
        job_config,
        scheduler,
        parent_context,
        output_path,
        action_runner,
        eventbus_publish,
    ):
        """Factory method to create a new Job instance from configuration."""
        action_graph = actiongraph.ActionGraph.from_config(
            job_config.actions,
            job_config.cleanup_action,
        )
        runs = jobrun.JobRunCollection.from_config(job_config, eventbus_publish)
        node_repo = node.NodePoolRepository.get_instance()

        return cls(
            name=job_config.name,
            monitoring=job_config.monitoring,
            time_zone=job_config.time_zone,
            queueing=job_config.queueing,
            all_nodes=job_config.all_nodes,
            node_pool=node_repo.get_by_name(job_config.node),
            scheduler=scheduler,
            eventbus_publish=eventbus_publish,
            enabled=job_config.enabled,
            run_collection=runs,
            action_graph=action_graph,
            parent_context=parent_context,
            output_path=output_path,
            allow_overlap=job_config.allow_overlap,
            action_runner=action_runner,
            max_runtime=job_config.max_runtime,
            expected_runtime=job_config.expected_runtime,
        )

    def update_from_job(self, job):
        """Update this Jobs configuration from a new config. This method
        actually takes an already constructed job and copies out its
        configuration data.
        """
        for attr in self.equality_attributes:
            setattr(self, attr, getattr(job, attr))
        log.info(f'{self} reconfigured')

    @property
    def status(self):
        """Current status."""
        if not self.enabled:
            return self.STATUS_DISABLED
        if self.runs.get_run_by_state(ActionRun.STATE_RUNNING):
            return self.STATUS_RUNNING

        if self.runs.get_run_by_state(ActionRun.STATE_SCHEDULED):
            return self.STATUS_ENABLED

        log.warning("%s in an unknown state: %s" % (self, self.runs))
        return self.STATUS_UNKNOWN

    def get_name(self):
        return self.name

    def get_monitoring(self):
        return self.monitoring

    def get_time_zone(self):
        return self.time_zone

    def get_runs(self):
        return self.runs

    @property
    def state_data(self):
        """This data is used to serialize the state of this job."""
        return {
            'runs': self.runs.state_data,
            'enabled': self.enabled,
        }

    def get_job_runs_from_state(self, state_data):
        """Apply a previous state to this Job."""
        self.enabled = state_data['enabled']
        job_runs = jobrun.job_runs_from_state(
            state_data['runs'],
            self.action_graph,
            self.output_path.clone(),
            self.context,
            self.node_pool,
            eventbus_publish=self.eventbus_publish,
        )
        return job_runs

    def build_new_runs(self, run_time, manual=False):
        """Uses its JobCollection to build new JobRuns. If all_nodes is set,
        build a run for every node, otherwise just builds a single run on a
        single node.
        """
        pool = self.node_pool
        nodes = pool.nodes if self.all_nodes else [pool.next()]
        for n in nodes:
            run = self.runs.build_new_run(self, run_time, n, manual=manual)
            self.watch(run)
            yield run

    def handle_job_run_state_change(self, _job_run, event):
        """Handle state changes from JobRuns and propagate changes to any
        observers.
        """
        # Propagate state change for serialization
        if event == jobrun.JobRun.NOTIFY_STATE_CHANGED:
            self.notify(self.NOTIFY_STATE_CHANGE)
            return

        # Propagate DONE JobRun notifications to JobScheduler
        if event == jobrun.JobRun.NOTIFY_DONE:
            self.notify(self.NOTIFY_RUN_DONE)
            return

    handler = handle_job_run_state_change

    def __eq__(self, other):
        return all(
            getattr(other, attr, None) == getattr(self, attr, None)
            for attr in self.equality_attributes
        )

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Job:%s" % self.name


class JobScheduler(Observer):
    """A JobScheduler is responsible for scheduling Jobs and running JobRuns
    based on a Jobs configuration. Runs jobs by setting a callback to fire
    x seconds into the future.
    """

    def __init__(self, job):
        self.job = job
        self.watch(job)

    def restore_state(self, job_state_data, config_action_runner):
        """Restore the job state and schedule any JobRuns."""
        job_runs = self.job.get_job_runs_from_state(job_state_data)
        for run in job_runs:
            self.job.watch(run)
        self.job.runs.runs.extend(job_runs)
        log.info(f'{self} restored')

        recovery.launch_recovery_actionruns_for_job_runs(
            job_runs=job_runs, master_action_runner=config_action_runner
        )

        scheduled = self.job.runs.get_scheduled()
        # for those that were already scheduled, we reschedule them to run.
        for job_run in scheduled:
            self._set_callback(job_run)

        # Ensure we have at least 1 scheduled run
        self.schedule()

    def enable(self):
        """Enable the job and start its scheduling cycle."""
        if self.job.enabled:
            return

        self.job.enabled = True
        self.create_and_schedule_runs(ignore_last_run_time=True)

    def create_and_schedule_runs(self, ignore_last_run_time=False):
        for job_run in self.get_runs_to_schedule(ignore_last_run_time):
            self._set_callback(job_run)
        # Eagerly save new runs in case tron gets restarted
        self.job.notify(Job.NOTIFY_STATE_CHANGE)

    def disable(self):
        """Disable the job and cancel and pending scheduled jobs."""
        self.job.enabled = False
        self.job.runs.cancel_pending()

    def manual_start(self, run_time=None):
        """Trigger a job run manually (instead of from the scheduler)."""
        run_time = run_time or timeutils.current_time(tz=self.job.time_zone)
        manual_runs = list(self.job.build_new_runs(run_time, manual=True))
        for r in manual_runs:
            r.start()
        return manual_runs

    def schedule_reconfigured(self):
        """Remove the pending run and create new runs with the new JobScheduler.
        """
        self.job.runs.remove_pending()
        self.create_and_schedule_runs(ignore_last_run_time=True)

    def schedule(self):
        """Schedule the next run for this job by setting a callback to fire
        at the appropriate time.
        """
        if not self.job.enabled:
            return
        self.create_and_schedule_runs()

    def _set_callback(self, job_run):
        """Set a callback for JobRun to fire at the appropriate time."""
        seconds = job_run.seconds_until_run_time()
        human_time = humanize.naturaltime(seconds, future=True)
        log.info(f"Scheduling {job_run} {human_time} ({seconds} seconds)")
        eventloop.call_later(seconds, self.run_job, job_run)

    # TODO: new class for this method
    def run_job(self, job_run, run_queued=False):
        """Triggered by a callback to actually start the JobRun. Also
        schedules the next JobRun.
        """
        # If the Job has been disabled after this run was scheduled, then cancel
        # the JobRun and do not schedule another
        if not self.job.enabled:
            log.info(f"Cancelled {job_run} because job has been disabled.")
            return job_run.cancel()

        # If the JobRun was cancelled we won't run it.  A JobRun may be
        # cancelled if the job was disabled, or manually by a user. It's
        # also possible this job was run (or is running) manually by a user.
        # Alternatively, if run_queued is True, this job_run is already queued.
        if not run_queued and not job_run.is_scheduled:
            log.info(
                f"{job_run} in state {job_run.state} is not scheduled, "
                "scheduling a new run instead of running"
            )
            return self.schedule()

        node = job_run.node if self.job.all_nodes else None
        # If there is another job run still running, queue or cancel this one
        if not self.job.allow_overlap and any(self.job.runs.get_active(node)):
            self._queue_or_cancel_active(job_run)
            return

        job_run.start()
        self.schedule_termination(job_run)
        if not self.job.scheduler.schedule_on_complete:
            self.schedule()

    def schedule_termination(self, job_run):
        if self.job.max_runtime:
            seconds = timeutils.delta_total_seconds(self.job.max_runtime)
            eventloop.call_later(seconds, job_run.stop)

    def _queue_or_cancel_active(self, job_run):
        if self.job.queueing:
            log.info(f"{self.job} still running, queueing {job_run}")
            return job_run.queue()

        log.info(f"{self.job} still running, cancelling {job_run}")
        job_run.cancel()
        self.schedule()

    def handle_job_events(self, _observable, event):
        """Handle notifications from observables. If a JobRun has completed
        look for queued JobRuns that may need to start now.
        """
        if event != Job.NOTIFY_RUN_DONE:
            return
        self.run_queue_schedule()

    def run_queue_schedule(self):
        # TODO: this should only start runs on the same node if this is an
        # all_nodes job, but that is currently not possible
        queued_run = self.job.runs.get_first_queued()
        if queued_run:
            eventloop.call_later(0, self.run_job, queued_run, run_queued=True)

        # Attempt to schedule a new run.  This will only schedule a run if the
        # previous run was cancelled from a scheduled state, or if the job
        # scheduler is `schedule_on_complete`.
        self.schedule()

    handler = handle_job_events

    def get_runs_to_schedule(self, ignore_last_run_time):
        """Build and return the runs to schedule."""
        if self.job.runs.has_pending:
            log.info(f"{self.job} has pending runs, can't schedule more.")
            return []

        if ignore_last_run_time:
            last_run_time = None
        else:
            last_run = self.job.runs.get_newest(include_manual=False)
            last_run_time = last_run.run_time if last_run else None
        next_run_time = self.job.scheduler.next_run_time(last_run_time)
        return self.job.build_new_runs(next_run_time)

    def __str__(self):
        return f"{self.__class__.__name__}({self.job})"

    def get_name(self):
        return self.job.name

    def get_job(self):
        return self.job

    def get_job_runs(self):
        return self.job.runs

    def __eq__(self, other):
        return bool(other and self.get_job() == other.get_job())

    def __ne__(self, other):
        return not self == other


class JobSchedulerFactory(object):
    """Construct JobScheduler instances from configuration."""

    def __init__(self, context, output_stream_dir, time_zone, action_runner, eventbus_publish):
        self.context = context
        self.output_stream_dir = output_stream_dir
        self.time_zone = time_zone
        self.action_runner = action_runner
        self.eventbus_publish = eventbus_publish

    def build(self, job_config):
        log.debug(f"Building new job {job_config.name}")
        output_path = filehandler.OutputPath(self.output_stream_dir)
        time_zone = job_config.time_zone or self.time_zone
        scheduler = scheduler_from_config(job_config.schedule, time_zone)
        job = Job.from_config(
            job_config=job_config,
            scheduler=scheduler,
            parent_context=self.context,
            output_path=output_path,
            action_runner=self.action_runner,
            eventbus_publish=self.eventbus_publish,
        )
        return JobScheduler(job)


class JobCollection(object):
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

    def update(self, new_job_scheduler):
        log.info(f"Updating {new_job_scheduler}")
        job_scheduler = self.get_by_name(new_job_scheduler.get_name())
        job_scheduler.get_job().update_from_job(new_job_scheduler.get_job())
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
