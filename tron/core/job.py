import logging
import itertools

from tron import command_context, event, node, eventloop
from tron.core import jobrun
from tron.core import actiongraph
from tron.core.actionrun import ActionRun
from tron.scheduler import scheduler_from_config
from tron.serialize import filehandler
from tron.utils import timeutils, proxy, iteration, collections
from tron.utils.observer import Observable, Observer

class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger(__name__)


class JobState(Observable):
    """A configurable data object.

    Job uses JobRunCollection to manage its runs, and ActionGraph to manage its
    actions and their dependency graph.
    """

    STATUS_DISABLED         = "disabled"
    STATUS_ENABLED          = "enabled"
    STATUS_UNKNOWN          = "unknown"
    STATUS_RUNNING          = "running"

    NOTIFY_STATUS_CHANGE    = 'notify_status_change'

    # NOTIFY_STATE_CHANGE     = 'notify_state_change'
    # NOTIFY_RUN_DONE         = 'notify_run_done'

    # context_class           = command_context.JobContext

    # These attributes determine equality between two Job objects
    # equality_attributes = [
    #     'name',
    #     'queueing',
    #     'scheduler',
    #     'node_pool',
    #     'all_nodes',
    #     'action_graph',
    #     'output_path',
    #     'action_runner',
    #     'max_runtime',
    #     'allow_overlap',
    # ]

    # # TODO: use config object
    # def __init__(self, name, scheduler, queueing=True, all_nodes=False,
    #         node_pool=None, enabled=True, action_graph=None,
    #         run_collection=None, parent_context=None, output_path=None,
    #         allow_overlap=None, action_runner=None, max_runtime=None,
    #         config=None):
    #     super(Job, self).__init__()
    #     self.name               = name
    #     self.action_graph       = action_graph
    #     self.scheduler          = scheduler
    #     self.runs               = run_collection
    #     self.queueing           = queueing
    #     self.all_nodes          = all_nodes
    #     self.enabled            = enabled
    #     self.node_pool          = node_pool
    #     self.allow_overlap      = allow_overlap
    #     self.action_runner      = action_runner
    #     self.max_runtime        = max_runtime
    #     self.config             = config
    #     self.output_path        = output_path or filehandler.OutputPath()
    #     self.output_path.append(name)
    #     self.event              = event.get_recorder(self.name)
    #     self.context = command_context.build_context(self, parent_context)
    #     self.event.ok('created')

    def __init__(self, enabled, name):
        super(JobState, self).__init__()
        self.enabled = enabled
        self.name = name

    # @classmethod
    # def from_config(cls,
    #         job_config, scheduler, parent_context, output_path, action_runner):
    #     """Factory method to create a new Job instance from configuration."""
    #     action_graph = actiongraph.ActionGraph.from_config(
    #             job_config.actions, job_config.cleanup_action)
    #     runs         = jobrun.JobRunCollection.from_config(job_config)
    #     node_repo    = node.NodePoolRepository.get_instance()

    #     return cls(
    #         name                = job_config.name,
    #         queueing            = job_config.queueing,
    #         all_nodes           = job_config.all_nodes,
    #         node_pool           = node_repo.get_by_name(job_config.node),
    #         scheduler           = scheduler,
    #         enabled             = job_config.enabled,
    #         run_collection      = runs,
    #         action_graph        = action_graph,
    #         parent_context      = parent_context,
    #         output_path         = output_path,
    #         allow_overlap       = job_config.allow_overlap,
    #         action_runner       = action_runner,
    #         max_runtime         = job_config.max_runtime,
    #         config              = job_config)

    # def update_from_job(self, job):
    #     """Update this Jobs configuration from a new config. This method
    #     actually takes an already constructed job and copies out its
    #     configuration data.
    #     """
    #     for attr in self.equality_attributes:
    #         setattr(self, attr, getattr(job, attr))
    #     self.event.ok('reconfigured')

    def enable(self):
        self.enabled = True
        self.notify(self.NOTIFY_STATUS_CHANGE)

    def disable(self):
        self.enabled = False
        self.notify(self.NOTIFY_STATUS_CHANGE)

    def status(self, job_runs):
        """Current status."""
        if not self.enabled:
            return self.STATUS_DISABLED
        if job_runs.get_run_by_state(ActionRun.STATE_RUNNING):
            return self.STATUS_RUNNING

        if (job_runs.get_run_by_state(ActionRun.STATE_SCHEDULED) or
                job_runs.get_run_by_state(ActionRun.STATE_QUEUED)):
            return self.STATUS_ENABLED

        log.warn("%s in an unknown state: %s" % (self, self.runs))
        return self.STATUS_UNKNOWN

    # def get_name(self):
    #     return self.name

    # def get_runs(self):
    #     return self.runs

    @property
    def state_data(self):
        """This data is used to serialize the state of this job."""
        return {
            #'runs':             self.runs.state_data,
            'enabled':          self.enabled,
            'name':             self.name
        }

    def restore_state(self, state_data):
        """Apply a previous state to this Job."""
        self.enabled = state_data['enabled']
        # job_runs = self.runs.restore_state(
        #         state_data['runs'],
        #         self.action_graph,
        #         self.output_path.clone(),
        #         self.context,
        #         self.node_pool)
        # for run in job_runs:
        #     self.watch(run)

        # self.event.ok('restored')

    # def build_new_runs(self, run_time, manual=False):
    #     """Uses its JobCollection to build new JobRuns. If all_nodes is set,
    #     build a run for every node, otherwise just builds a single run on a
    #     single node.
    #     """
    #     pool = self.node_pool
    #     nodes = pool.nodes if self.all_nodes else [pool.next()]
    #     for node in nodes:
    #         run = self.runs.build_new_run(self, run_time, node, manual=manual)
    #         self.watch(run)
    #         yield run

    # def handle_job_run_state_change(self, _job_run, event):
    #     """Handle state changes from JobRuns and propagate changes to any
    #     observers.
    #     """
    #     # Propagate state change for serialization
    #     if event == jobrun.JobRun.NOTIFY_STATE_CHANGED:
    #         self.notify(self.NOTIFY_STATE_CHANGE)
    #         return

    #     # Propagate DONE JobRun notifications to JobScheduler
    #     if event == jobrun.JobRun.NOTIFY_DONE:
    #         self.notify(self.NOTIFY_RUN_DONE)
    #         return
    # handler = handle_job_run_state_change

    def __eq__(self, other):
        return getattr(self, 'name', None) == getattr(other, 'name', None)

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Job:%s" % self.name


class JobScheduler(Observer):
    """A JobScheduler is responsible for scheduling and running JobRuns
    based on a Jobs configuration. Runs jobs by setting a callback to fire
    x seconds into the future.
    """

    def __init__(self, job_runs, job_config, scheduler, actiongraph, nodes, path, context, watcher, actionrunner):
        self.job_runs           = job_runs
        self.config             = job_config
        self.enabled            = job_config.enabled
        self.scheduler          = scheduler
        self.action_graph       = actiongraph
        self.action_runner      = actionrunner
        self.node_pool          = nodes
        self.output_path        = path or filehandler.OutputPath()
        self.output_path.append(job_config.name)
        self.context            = context
        self.watcher            = watcher
        self.shutdown_requested = False
        self.proxy              = proxy.AttributeProxy(self.config,[
            'all_nodes',
            'allow_overlap',
            'name',
            'queueing',
            'max_runtime'
            ])
        #self.watch(job)

    def restore_state(self, job_state_data):
        """Restore the job state and schedule any JobRuns."""
        #self.job.restore_state(job_state_data)
        self.enabled = job_state_data['enabled']
        scheduled = self.job_runs.get_scheduled()
        for job_run in scheduled:
            self._set_callback(job_run)
        # Ensure we have at least 1 scheduled run
        self.schedule()

    def enable(self):
        """Enable the job and start its scheduling cycle."""
        if self.enabled:
            return

        self.enabled = True
        self.create_and_schedule_runs(ignore_last_run_time=True)

    def create_and_schedule_runs(self, ignore_last_run_time=False):
        for job_run in self.get_runs_to_schedule(ignore_last_run_time):
            self._set_callback(job_run)

    def disable(self):
        """Disable the job and cancel and pending scheduled jobs."""
        self.enabled = False
        self.job_runs.cancel_pending()

    @property
    def is_shutdown(self):
        """Return True if there are no running or starting runs."""
        return not any(self.job_runs.get_active())

    def manual_start(self, run_time=None):
        """Trigger a job run manually (instead of from the scheduler)."""
        run_time = run_time or timeutils.current_time()
        manual_runs = list(self.build_new_runs(run_time, manual=True))
        for r in manual_runs:
            r.start()

    def build_new_runs(self, run_time, manual=False):
        """Uses its JobCollection to build new JobRuns. If all_nodes is set,
        build a run for every node, otherwise just builds a single run on a
        single node.
        """
        pool = self.node_pool
        nodes = pool.nodes if self.config.all_nodes else [pool.next()]
        for node in nodes:
            run = self.job_runs.build_new_run(self, run_time, node, manual=manual)
            self.watcher.watch(run, jobrun.JobRun.NOTIFY_STATE_CHANGED)
            self.watch(run, jobrun.JobRun.NOTIFY_DONE)
            yield run

    def schedule_reconfigured(self):
        """Remove the pending run and create new runs with the new JobScheduler.
        """
        self.job_runs.remove_pending()
        self.create_and_schedule_runs(ignore_last_run_time=True)

    def schedule(self):
        """Schedule the next run for this job by setting a callback to fire
        at the appropriate time.
        """
        if not self.enabled:
            return
        self.create_and_schedule_runs()

    def _set_callback(self, job_run):
        """Set a callback for JobRun to fire at the appropriate time."""
        log.info("Scheduling next Jobrun for %s", self.config.name)
        seconds = job_run.seconds_until_run_time()
        eventloop.call_later(seconds, self.run_job, job_run)

    # TODO: new class for this method
    def run_job(self, job_run, run_queued=False):
        """Triggered by a callback to actually start the JobRun. Also
        schedules the next JobRun.
        """
        if self.shutdown_requested:
            return

        # If the Job has been disabled after this run was scheduled, then cancel
        # the JobRun and do not schedule another
        if not self.enabled:
            log.info("%s cancelled because job has been disabled." % job_run)
            return job_run.cancel()

        # If the JobRun was cancelled we won't run it.  A JobRun may be
        # cancelled if the job was disabled, or manually by a user. It's
        # also possible this job was run (or is running) manually by a user.
        # Alternatively, if run_queued is True, this job_run is already queued.
        if not run_queued and not job_run.is_scheduled:
            log.info("%s in state %s already out of scheduled state." % (
                    job_run, job_run.state))
            return self.schedule()

        node = job_run.node if self.config.all_nodes else None
        # If there is another job run still running, queue or cancel this one
        if not self.config.allow_overlap and any(self.job_runs.get_active(node)):
            self._queue_or_cancel_active(job_run)
            return

        job_run.start()
        self.schedule_termination(job_run)
        if not self.scheduler.schedule_on_complete:
            self.schedule()

    def schedule_termination(self, job_run):
        if self.config.max_runtime:
            seconds = timeutils.delta_total_seconds(self.config.max_runtime)
            eventloop.call_later(seconds, job_run.stop)

    def _queue_or_cancel_active(self, job_run):
        if self.config.queueing:
            log.info("Job:%s still running, queueing %s." % (self.config.name, job_run))
            return job_run.queue()

        log.info("Job:%s still running, cancelling %s." % (self.config.name, job_run))
        job_run.cancel()
        self.schedule()
    # This is now also called when a JobRun does a .notify
    def handle_job_events(self, _observable, event):
        """Handle notifications from observables. If a JobRun has completed
        look for queued JobRuns that may need to start now.
        """
        if event != jobrun.JobRun.NOTIFY_DONE:
            return

        # TODO: this should only start runs on the same node if this is an
        # all_nodes job, but that is currently not possible
        queued_run = self.job_runs.get_first_queued()
        if queued_run:
            eventloop.call_later(0, self.run_job, queued_run, run_queued=True)

        # Attempt to schedule a new run.  This will only schedule a run if the
        # previous run was cancelled from a scheduled state, or if the job
        # scheduler is `schedule_on_complete`.
        self.schedule()
    handler = handle_job_events

    def get_runs_to_schedule(self, ignore_last_run_time):
        """Build and return the runs to schedule."""
        if self.job_runs.has_pending:
            log.info("Job:%s has pending runs, can't schedule more." % self.config.name)
            return []

        if ignore_last_run_time:
            last_run_time = None
        else:
            last_run = self.job_runs.get_newest(include_manual=False)
            last_run_time = last_run.run_time if last_run else None
        next_run_time = self.scheduler.next_run_time(last_run_time)
        return self.build_new_runs(next_run_time)

    def request_shutdown(self):
        self.shutdown_requested = True

    def __str__(self):
        return "%s(Job:%s)" % (self.__class__.__name__, self.config.name)

    def get_name(self):
        return self.config.name

    # def get_job(self):
    #     return self.job

    def get_job_runs(self):
        return self.job_runs

    def __getattr__(self, name):
        return self.proxy.perform(name)

    # TODO: needs a bit stronger comparison
    def __eq__(self, other):
        return bool(getattr(self, 'config', None) == getattr(other, 'config', None))

    def __ne__(self, other):
        return not self == other


class JobSchedulerFactory(object):
    """Construct JobScheduler instances from configuration."""

    context_class           = command_context.JobContext

    def __init__(self, context, output_stream_dir, time_zone, action_runner):
        self.context            = context
        self.output_stream_dir  = output_stream_dir
        self.time_zone          = time_zone
        self.action_runner      = action_runner

    def build(self, job_config, job_runs, actiongraph, nodes, watcher, actionrunner):
        log.debug("Building new job %s", job_config.name)
        output_path = filehandler.OutputPath(self.output_stream_dir)
        scheduler = scheduler_from_config(job_config.schedule, self.time_zone)
        context = command_context.build_context(self, self.context)
        #job = Job.from_config(job_config, scheduler, self.context, output_path, self.action_runner)
        return JobScheduler(job_runs, job_config, scheduler, actiongraph,
            nodes, output_path, context, watcher, actionrunner)


class JobCollection(object):
    """A collection of jobs."""

    def __init__(self):
        self.jobs = collections.MappingCollection('jobs')
        self.proxy = proxy.CollectionProxy(self.jobs.itervalues, [
            proxy.func_proxy('request_shutdown',    iteration.list_all),
            proxy.func_proxy('enable',              iteration.list_all),
            proxy.func_proxy('disable',             iteration.list_all),
            proxy.func_proxy('schedule',            iteration.list_all),
            proxy.attr_proxy('is_shutdown',         all)
        ])

    def load_from_config(self, job_configs, factory, reconfigure, state_watcher):
        """Apply a configuration to this collection and return a generator of
        jobs which were added.
        """
        self.jobs.filter_by_name(job_configs)

        def map_to_job_and_schedule(job_containers):
            for job_container in job_containers:
                if reconfigure:
                    job_container.schedule()
                yield job_container.get_job_state()

        seq = (JobContainer.from_config(config, factory, state_watcher) for config in job_configs.itervalues())
        return map_to_job_and_schedule(itertools.ifilter(self.add, seq))

    def add(self, job_scheduler):
        return self.jobs.add(job_scheduler, self.update)

    def update(self, new_job_container):
        log.info("Updating %s", new_job_container)
        job_container = self.get_by_name(new_job_container.get_name())
        job_container.update_from_job(new_job_container)
        job_container.schedule_reconfigured()
        return True

    def restore_state(self, job_state_data):
        self.jobs.restore_state(job_state_data)

    def get_by_name(self, name):
        return self.jobs.get(name)

    def get_jobs_by_namespace(self, namespace):
        return [job for job in self.get_jobs()
            if job.namespace == namespace]

    def get_names(self):
        return self.jobs.keys()

    def get_jobs(self):
        return [container for container in self]

    def get_job_run_collections(self):
        return [container.get_job_runs() for container in self]

    def __iter__(self):
        return self.jobs.itervalues()

    def __getattr__(self, name):
        return self.proxy.perform(name)

    def __contains__(self, name):
        return name in self.jobs


class JobContainer(object):

    equality_attributes = [
        'config',
        'scheduler',
        'node_pool',
        'action_graph',
        'output_path',
    ]

    # STATUS_DISABLED         = "disabled"
    # STATUS_ENABLED          = "enabled"
    # STATUS_UNKNOWN          = "unknown"
    # STATUS_RUNNING          = "running"

    # NOTIFY_STATE_CHANGE     = 'notify_state_change'
    # NOTIFY_RUN_DONE         = 'notify_run_done'

    # context_class           = command_context.JobContext

    def __init__(self, jobstate, jobruns, jobscheduler, statewatcher):
        self.job_state       = jobstate
        self.job_runs        = jobruns
        self.job_scheduler   = jobscheduler
        self.watcher         = statewatcher
        self.event           = event.get_recorder(jobscheduler.config.name)
        self.proxy           = proxy.AttributeProxy(self.job_scheduler, [
            'schedule',
            'schedule_reconfigured',
            'request_shutdown',
            'manual_start',
            'is_shutdown',
            'action_graph',
            'node_pool',
            'output_path',
            'all_nodes',
            'allow_overlap',
            'max_runtime',
            'name',
            'queueing',
            'scheduler',
            'action_runner',
            'enabled'
        ])

    @classmethod
    def from_config(cls, config, factory, statewatcher):
        runs = jobrun.JobRunCollection.from_config(config)
        action_graph = actiongraph.ActionGraph.from_config(
            config.actions, config.cleanup_action)
        node_pool = node.NodePoolRepository.get_instance().get_by_name(config.node)
        scheduler = factory.build(config, runs, action_graph, node_pool, statewatcher, factory.action_runner)
        job_state = JobState(config.enabled, config.name)
        statewatcher.watch(job_state)
        return cls(job_state, runs, scheduler, statewatcher)

    def restore_state(self, job_state_data):
        job_runs = self.job_runs.restore_state(
            job_state_data['runs'],
            self.action_graph,
            self.output_path.clone(),
            self.context,
            self.node_pool)
        for run in job_runs:
            self.watcher.watch(run)
        self.job_scheduler.restore_state(job_state_data)
        self.job_state.restore_state(job_state_data) #just make sure flag is changed
        self.event.ok('reconfigured')

    def update_from_job(self, job):
        """Update this Job's configuration from a new config. This method
        actually takes an already constructed job and copies out its
        configuration data.
        """
        for attr in self.equality_attributes:
            setattr(self, attr, getattr(job, attr))
        self.event.ok('reconfigured')

    def enable(self):
        self.job_state.enable()
        self.job_scheduler.enable()

    def disable(self):
        self.job_state.disable()
        self.job_scheduler.disable()

    @property
    def namespace(self):
        return self.job_scheduler.config.namespace

    @property
    def status(self):
        return self.job_state.status(self.job_runs)

    def get_name(self):
        return self.job_state.name

    def get_job_runs(self):
        return [run for run in self.job_runs]

    def get_job_state(self):
        return self.job_state

    def __getattr__(self, name):
        return self.proxy.perform(name)

    def __eq__(self, other):
        return all(getattr(self, attr, None) == getattr(other, attr, None)
            for attr in self.equality_attributes)

    def __str__(self):
        return "%s" % self.job_scheduler
