import logging
from collections import deque
from twisted.internet import reactor

from tron import command_context, event
from tron.core import action, jobrun
from tron.core import actiongraph
from tron.core.actionrun import ActionRun
from tron.utils import timeutils
from tron.utils.observer import Observable, Observer

class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger('tron.core.job')


class JobContext(object):
    """A class which exposes properties for rendering commands."""

    def __init__(self, job):
        self.job = job

    @property
    def name(self):
        return self.job.name

    # TODO: test
    def __getitem__(self, item):
        if ':' not in  item:
            raise KeyError(item)

        date_name, date_spec = item.split(':', 1)
        if date_name == 'last_success':
            time_value = timeutils.DateArithmetic.parse(date_spec)
            if time_value:
                return time_value

        raise KeyError(item)


class Job(Observable, Observer):
    """A configurable data object.

    Job uses JobRunCollection to manage its runs, and ActionGraph to manage its
    actions and their dependency graph.
    """

    STATUS_DISABLED         = "DISABLED"
    STATUS_ENABLED          = "ENABLED"
    STATUS_UNKNOWN          = "UNKNOWN"
    STATUS_RUNNING          = "RUNNING"

    EVENT_STATE_CHANGE      = 'event_state_change'
    EVENT_RECONFIGURED      = event.EventType(event.LEVEL_INFO, 'reconfigured')
    EVENT_STATE_RESTORED    = 'event_state_restored'

    def __init__(self, name, scheduler, queueing=True, all_nodes=False,
            node_pool=None, enabled=True, action_graph=None,
            cleanup_action=None, run_collection=None, parent_context=None,
            output_path=None):
        super(Job, self).__init__()
        self.name               = name
        self.action_graph       = action_graph
        self.cleanup_action     = cleanup_action
        self.scheduler          = scheduler
        self.runs               = run_collection
        self.queueing           = queueing
        self.all_nodes          = all_nodes
        self.enabled            = enabled
        self.node_pool          = node_pool
        self.context            = command_context.CommandContext(
                                    JobContext(self), parent_context)
        self.output_path        = output_path
        self.output_path.append(name)

    @classmethod
    def from_config(cls,
                job_config, node_pools, scheduler, parent_context, output_path):
        """Factory method to create a new Job instance from configuration."""
        action_graph = actiongraph.ActionGraph.from_config(
                job_config.actions, node_pools)
        runs = jobrun.JobRunCollection.from_config(job_config)

        cleanup_action = None
        if job_config.cleanup_action:
            cleanup_action = action.Action.from_config(
                    job_config.cleanup_action, node_pools)

        nodes = node_pools[job_config.node] if job_config.node else None

        return cls(
            name                = job_config.name,
            queueing            = job_config.queueing,
            all_nodes           = job_config.all_nodes,
            node_pool           = nodes,
            scheduler           = scheduler,
            enabled             = job_config.enabled,
            run_collection      = runs,
            action_graph        = action_graph,
            cleanup_action      = cleanup_action,
            parent_context      = parent_context,
            output_path         = output_path
        )

    def update_from_job(self, job, nodes):
        """Update this Jobs configuration for a new config. This method
        actually takes an already constructed job and copies out its
        configuration data.
        """
        # TODO: test with __eq__
        self.enabled    = job.enabled
        self.all_nodes  = job.all_nodes
        self.queueing   = job.queueing
        self.node_pool  = nodes[job.node] if job.node else None
        # TODO: copy parent_context
        # TODO: update action_graph
        # TODO: copy output_path
        # TODO: update JobRunCollection
        self.notify(self.EVENT_RECONFIGURED)

    @property
    def status(self):
        """The Jobs current status is determined by its last/next run."""
        if not self.enabled:
            return self.STATUS_DISABLED
        if self.runs.get_run_by_state(ActionRun.STATE_RUNNING):
            return self.STATUS_RUNNING

        if (self.runs.get_run_by_state(ActionRun.STATE_SCHEDULED) or
                self.runs.get_run_by_state(ActionRun.STATE_QUEUED)):
            return self.STATUS_ENABLED

        # TODO: log what unknown state
        return self.STATUS_UNKNOWN

    def repr_data(self):
        """Returns a dict that is the external representation of this job."""
        last_success = self.runs.last_success
        return {
            'name':             self.name,
            'scheduler':        str(self.scheduler),
            'action_names':     self.action_graph.names,
            'node_pool':        [n.hostname for n in self.node_pool.nodes],
            'status':           self.status,
            'last_success':     last_success.end_time if last_success else None,
        }

    @property
    def state_data(self):
        """This data is used to serialize the state of this job."""
        return {
            'runs':             self.runs.state_data,
            'enabled':          self.enabled
        }

    def restore_state(self, state_data):
        """Apply a previous state to this Job."""
        self.enabled = state_data['enabled']
        job_runs = self.runs.restore_state(
                state_data['runs'], self.action_graph)
        for run in job_runs:
            self.watch(run, True)

        self.notify(self.EVENT_STATE_RESTORED)

    def build_new_runs(self, run_time):
        """Uses its JobCollection to build new JobRuns.. If all_nodes is set,
        build a run for every node, otherwise just builds a single run on a
        single node.
        """
        pool = self.node_pool
        nodes = pool.nodes if self.all_nodes else [pool.next()]
        for node in nodes:
            run = self.runs.build_new_run(self, run_time, node)
            self.watch(run, True)
            yield run

    def watcher(self, job_run, event):
        """Handle state changes from JobRuns and propagate changes to any
        observers.
        """
        # TODO: propagate state change for serialization
        # TODO: propagate finished JobRun notifications to JobScheduler

    def __eq__(self, other):
        # TODO: add runs and action_graph
        if (not isinstance(other, Job) or self.name != other.name or
            self.queueing != other.queueing or
            self.scheduler != other.scheduler or
            self.node_pool != other.node_pool or
            self.all_nodes != other.all_nodes or
            self.cleanup_action != other.cleanup_action):
            # TODO: compare base dir
            # TODO: compare context
            # TODO: EVERYTHING

            return False

        # TODO: replace with action graph
        return all([me == you for (me, you) in zip(self.topo_actions,
            other.topo_actions)])

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return "Job:%s" % self.name


class JobScheduler(Observer):
    """A JobScheduler is responsible for scheduling Jobs and running JobRuns
    based on a Jobs configuration.
    """

    def __init__(self):
        pass

    def enable(self, job):
        self.enabled = True
        self.run_or_schedule()

    def disable(self, job):
        self.enabled = False
        self.runs.cancel_pending()

    def run_or_schedule(self):
        """Called to either run a currently scheduled/queued job, or if none
        are scheduled/queued, create a new scheduled run.
        """
        # TODO


    def get_runs_to_schedule(self):
        """If the scheduler is just a 'best effort' scheduler and this job has
        queued runs, we don't need to schedule any more yet. Otherwise schedule
        the next run.
        """
        best_effort = self.scheduler.is_best_effort

        if best_effort and self.runs.get_run_by_state(ActionRun.STATE_QUEUED):
            return None

        if best_effort and self.runs.get_run_by_state(ActionRun.STATE_SCHEDULED):
            return None

        return self.next_runs()

    # TODO: DELETE
    def next_runs(self):
        """Use the configured scheduler to build the next job runs.  If there
        are runs already scheduled, return those."""
        if not self.scheduler:
            return []

        last_run_time = None
        if self.runs:
            last_run_time = self.runs[0].run_time

        next_run_time = self.scheduler.next_run_time(last_run_time)
        return self.build_and_add_runs(next_run_time)

    def _schedule(self, run):
        secs = run.seconds_until_run_time()
        reactor.callLater(secs, self.run_job, run)

    def schedule_next_run(self):
        runs = self.get_runs_to_schedule() or []
        for next in runs:
            log.info("Scheduling next job for %s", next.job.name)
            self._schedule(next)

    def run_job(self, job_run):
        """This runs when a job was scheduled.
        Here we run the job and schedule the next time it should run
        """
        if not job_run.job:
            return

        # TODO: do these belong here?
        if not job_run.job.enabled:
            return

        job_run.scheduled_start()
        self.schedule_next_run()

    def manual_start(self, run_time=None):
        """Trigger a job run manually (instead of from the scheduler)."""
        run_time = run_time or timeutils.current_time()
        manual_runs = self.build_new_runs(run_time)

        # Insert this run before any scheduled runs
        scheduled = deque()
        while self.runs and self.runs[0].is_scheduled:
            scheduled.appendleft(self.runs.popleft())

        self.runs.extendleft(manual_runs)
        self.runs.extendleft(scheduled)

        for r in manual_runs:
            r.manual_start()
        return manual_runs

    # TODO:
    def schedule_reconfigured(self, job):
        """Called after a job has been reconfigured by reloading the config.
        If the job is enabled and the schedule has changed, cancel the
        pending run and create a new run with the correct schedule.
        """
