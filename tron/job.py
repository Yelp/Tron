import logging
import os
from collections import deque
from twisted.internet import reactor

from tron import  command_context, event
from tron.core.action import Action, ActionRun
from tron.core.actiongraph import ActionGraph
from tron.core.jobrun import JobRunCollection
from tron.scheduler import scheduler_from_config
from tron.utils import timeutils
from tron.utils.observer import Observable, Observer

class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger('tron.core.job')


class Job(Observable):
    """A configurable data object."""

    STATUS_DISABLED =   "DISABLED"
    STATUS_ENABLED =    "ENABLED"
    STATUS_UNKNOWN =    "UNKNOWN"
    STATUS_RUNNING =    "RUNNING"

    EVENT_STATE_CHANGE = 'event_state_change'
    EVENT_RECONFIGURED = 'event_reconfigured'

    def __init__(self, name=None, queueing=True, all_nodes=False,
            scheduler=None, node_pool=None, enabled=True, action_graph=None,
            cleanup_action=None, run_collection=None):
        super(Job, self).__init__()
        self.name = name
        self.action_graph = action_graph
        self.cleanup_action = cleanup_action
        self.scheduler = scheduler
        self.run_collection = run_collection

        self.queueing = queueing
        self.all_nodes = all_nodes
        self.enabled = enabled

        self.node_pool = node_pool
        # TODO: context and path ?


    @classmethod
    def from_config(cls, job_config, node_pools, scheduler):
        """Factory method to create a new Job instance from configuration."""
        pass

    def update_from_config(self, job_config, nodes):
        """Update this Jobs configuration from a new config."""
        # TODO: test with __eq__
        self.enabled    = job_config.enabled
        self.all_nodes  = job_config.all_nodes
        self.queueing   = job_config.queueing
        self.node_pool  = nodes[job_config.node] if job_config.node else None
        self.notify(self.EVENT_RECONFIGURED)

    def repr_data(self):
        """Returns a dict that is the external representation of this job."""
        last_success = self.last_success.end_time if self.last_success else None
        return {
            'name':             self.name,
            'scheduler':        str(self.scheduler),
            'action_names':     self.action_graph.names,
            'node_pool':        [n.hostname for n in self.node_pool.nodes],
            'status':           self.status,
            'last_success':     last_success,
            }

    @property
    def status(self):
        """The Jobs current status is determined by its last/next run."""
        # TODO: not a good way to determine this
        current_run = self.runs.get_next_to_finish()
        if not current_run:
            return self.STATUS_DISABLED
        if current_run.is_running:
            return self.STATUS_RUNNING
        if current_run.is_scheduled:
            return self.STATUS_ENABLED
        return self.STATUS_UNKNOWN

    @property
    def state_data(self):
        """This data is used to serialize the state of this job."""
        return {
            'runs':             self.runs.state_data,
            'enabled':          self.enabled
        }

    def restore_state(self, data):
        """Apply a previous state to this Job."""
        self.enabled = data['enabled']
        self.runs.restore_state(data['runs'])
        # TODO: can i change event_recorder to a single Observer?
        self.event_recorder.emit_info("restored")

    def __eq__(self, other):
        if (not isinstance(other, Job) or self.name != other.name or
            self.queueing != other.queueing or
            self.scheduler != other.scheduler or
            self.node_pool != other.node_pool or
            len(self.topo_actions) != len(other.topo_actions) or
            self.run_limit != other.run_limit or
            self.all_nodes != other.all_nodes or
            self.cleanup_action != other.cleanup_action):

            return False

        return all([me == you for (me, you) in zip(self.topo_actions,
            other.topo_actions)])

    def __ne__(self, other):
        return not self == other

class OldJobClass(object):

    def __init__(self, name=None, context=None, event_recorder=None,
        queueing=True, all_nodes=False, scheduler=None, node_pool=None,
        enabled=True, run_collection=None, action_graph=None,
        cleanup_action=None
    ):
        super(Job, self).__init__()
        self.name = name
        self.action_graph = action_graph
        self.cleanup_action = cleanup_action
        self.scheduler = scheduler
        self.runs = run_collection

        self.queueing = queueing
        self.all_nodes = all_nodes
        self.enabled = enabled

        self.node_pool = node_pool
        self.output_path = None
        self.context = command_context.CommandContext(self, context)
        self.event_recorder = event.EventRecorder(self, parent=event_recorder)

    @classmethod
    def from_config(cls, job_config, node_pools, time_zone):
        """Build a job from a ConfigJob."""

        action_graph = ActionGraph.from_config(job_config.actions, node_pools)
        runs = JobRunCollection.from_config(job_config)

        if job_config.cleanup_action:
            cleanup_action = Action.from_config(
                job_config.cleanup_action, node_pools)
        else:
            cleanup_action = None

        nodes = node_pools[job_config.node] if job_config.node else None
        scheduler = scheduler_from_config(job_config.schedule, time_zone)

        return cls(
            name=               job_config.name,
            queueing=           job_config.queueing,
            all_nodes=          job_config.all_nodes,
            node_pool=          nodes,
            scheduler=          scheduler,
            enabled=            job_config.enabled,
            run_collection=     runs,
            action_graph=       action_graph,
            cleanup_action=     cleanup_action
        )



    def notify_state_changed(self):
        """Called to notify this job that its state has changed due to an
        action run state change.  It will propagate this notification to
        any observers observing this Job.
        """
        self.notify(self.EVENT_STATE_CHANGE)

    def set_context(self, context):
        self.context.next = context

    def enable(self):
        self.enabled = True
        self.run_or_schedule()

    def run_or_schedule(self):
        """Called to either run a currently scheduled/queued job, or if none
        are scheduled/queued, create a new scheduled run.
        """
        # TODO

    def disable(self):
        self.enabled = False
        self.runs.cancel_pending()

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

    @property
    def last_success(self):
        """Last successful JobRun."""
        return self.runs.get_run_by_state(ActionRun.STATE_SUCCEEDED)

    def build_run(self, run_time, node=None, actions=None, run_num=None,
                  cleanup_action=None):
        """Create a JobRun with the specified run_time."""
        job_run = JobRun(self, run_time=run_time, run_num=run_num)

        job_run.node = node or self.node_pool.next()
        log.info("Built run %s", job_run.id)

        # Actions can be specified when restored from serialized state
        if not actions:
            actions = self.topo_actions

        self.build_action_dag(job_run, actions)

        cleanup_action = cleanup_action or self.cleanup_action
        if cleanup_action is not None:
            cleanup_action_run = self.cleanup_action.build_run(
                job_run, cleanup=True)
            job_run.cleanup_action_run = cleanup_action_run

        self.watch(job_run, JobRun.EVENT_DONE)
        return job_run

    def watcher(self, observable, event):
        """Watch for events from JobRuns."""
        if event == JobRun.EVENT_DONE:
            self.notify_job_run_complete()

    def notify_job_run_complete(self):
        """Called to notify this Job that one of its JobRuns has completed.
        This method will then check to see if a new job run needs to be
        scheduled and if one is scheduled, attempt to start it.
        """

        # TODO: fix this logic
        next = self.next_to_finish()
        if next and next.is_queued:
            next.attempt_start()

        # See if we need to scheduler another Run
        self.schedule_next_run()

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

    def build_new_runs(self, run_time):
        """Builds runs. If all_nodes is set, build a run for every node,
        otherwise just builds a single run on a single node.
        """
        if self.all_nodes:
            return [
                self.runs.build_new_run(self, run_time, node)
                for node in self.node_pool.nodes
            ]
        return [self.runs.build_run(self, run_time, self.node_pool.next())]

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



#    def restore_run(self, data):
#        action_names = []
#        for action in data['runs']:
#            action_names.append(action['id'].split('.')[-1])
#
#        def action_filter(topo_action):
#            return topo_action.name in action_names
#
#        action_list = filter(action_filter, self.topo_actions)
#
#        ca = (self.cleanup_action
#              if self.cleanup_action and action_filter(self.cleanup_action)
#              else None)
#        # TODO: this seems like it should be easier to restore a runs state
#        run = self.build_run(None, run_num=data['run_num'], actions=action_list,
#                             cleanup_action=ca)
#        self.run_num = max([run.run_num + 1, self.run_num])
#
#        run.restore(data)
#        self.runs.append(run)
#        return run

    def setup_job_dir(self, working_dir):
        """Setup a directory to store this jobs logs."""
        self.output_path = os.path.join(working_dir, self.name)
        if not os.path.exists(self.output_path):
            os.mkdir(self.output_path)

    # TODO: moved to JobState class


    def __str__(self):
        return "JOB:%s" % self.name


class JobScheduler(object):
    """
    A JobScheduler is responsible for scheduling and running runs based on its
    configuration. It acts as an Observer for its runs and an observable to
    propagate state changes back to the state serializer.

    Job uses JobRunCollection to manage its run, and ActionGraph to manage its
    actions and their dependency graph.
    """