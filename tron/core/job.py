import logging
from twisted.internet import reactor

from tron import command_context, event, node
from tron.core import jobrun
from tron.core import actiongraph
from tron.core.actionrun import ActionRun
from tron.serialize import filehandler
from tron.utils import timeutils
from tron.utils.observer import Observable, Observer

class Error(Exception):
    pass


class ConfigBuildMismatchError(Error):
    pass


class InvalidStartStateError(Error):
    pass


log = logging.getLogger(__name__)


class JobContext(object):
    """A class which exposes properties for rendering commands."""

    def __init__(self, job):
        self.job = job

    @property
    def name(self):
        return self.job.name

    def __getitem__(self, item):
        date_name, date_spec = self._get_date_spec_parts(item)
        if not date_spec:
            raise KeyError(item)

        if date_name == 'last_success':
            last_success = self.job.runs.last_success
            time_value = timeutils.DateArithmetic.parse(date_spec, last_success)
            if time_value:
                return time_value

        raise KeyError(item)

    def _get_date_spec_parts(self, name):
        parts = name.rsplit(':', 1)
        if len(parts) != 2:
            return [name, None]
        return parts


class Job(Observable, Observer):
    """A configurable data object.

    Job uses JobRunCollection to manage its runs, and ActionGraph to manage its
    actions and their dependency graph.
    """

    STATUS_DISABLED       = "DISABLED"
    STATUS_ENABLED        = "ENABLED"
    STATUS_UNKNOWN        = "UNKNOWN"
    STATUS_RUNNING        = "RUNNING"

    NOTIFY_STATE_CHANGE   = 'notify_state_change'
    NOTIFY_RUN_DONE       = 'notify_run_done'

    EVENT_RECONFIGURED    = event.EventType(event.LEVEL_NOTICE, 'reconfigured')
    EVENT_RUN_CREATED     = event.EventType(event.LEVEL_NOTICE, 'run_created')
    EVENT_STATE_RESTORED  = event.EventType(event.LEVEL_INFO, 'restored')

    def __init__(self, name, scheduler, queueing=True, all_nodes=False,
            node_pool=None, enabled=True, action_graph=None,
            run_collection=None, parent_context=None, output_path=None):
        super(Job, self).__init__()
        self.name               = name
        self.action_graph       = action_graph
        self.scheduler          = scheduler
        self.runs               = run_collection
        self.queueing           = queueing
        self.all_nodes          = all_nodes
        self.enabled            = enabled
        self.node_pool          = node_pool
        self.context            = command_context.CommandContext(
                                    JobContext(self), parent_context)
        self.output_path        = output_path or filehandler.OutputPath()
        self.output_path.append(name)

    @classmethod
    def from_config(cls, job_config, scheduler, parent_context, output_path):
        """Factory method to create a new Job instance from configuration."""
        node_pools = node.NodePoolStore.get_instance()
        action_graph = actiongraph.ActionGraph.from_config(
                job_config.actions, node_pools, job_config.cleanup_action)
        runs = jobrun.JobRunCollection.from_config(job_config)
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
            parent_context      = parent_context,
            output_path         = output_path
        )

    def update_from_job(self, job):
        """Update this Jobs configuration from a new config. This method
        actually takes an already constructed job and copies out its
        configuration data.
        """
        self.name           = job.name
        self.queueing       = job.queueing
        self.scheduler      = job.scheduler
        self.node_pool      = job.node_pool
        self.all_nodes      = job.all_nodes
        self.action_graph   = job.action_graph
        self.enabled        = job.enabled
        self.output_path    = job.output_path
        self.context        = job.context
        self.notify(self.EVENT_RECONFIGURED)

    @property
    def status(self):
        """Current status."""
        if not self.enabled:
            return self.STATUS_DISABLED
        if self.runs.get_run_by_state(ActionRun.STATE_RUNNING):
            return self.STATUS_RUNNING

        if (self.runs.get_run_by_state(ActionRun.STATE_SCHEDULED) or
                self.runs.get_run_by_state(ActionRun.STATE_QUEUED)):
            return self.STATUS_ENABLED

        log.warn("%s in an unknown state: %s" % (self, self.runs))
        return self.STATUS_UNKNOWN

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
                state_data['runs'],
                self.action_graph,
                self.output_path.clone(),
                self.context
        )
        for run in job_runs:
            self.watch(run)

        self.notify(self.EVENT_STATE_RESTORED)

    def build_new_runs(self, run_time, manual=False):
        """Uses its JobCollection to build new JobRuns. If all_nodes is set,
        build a run for every node, otherwise just builds a single run on a
        single node.
        """
        pool = self.node_pool
        nodes = pool.nodes if self.all_nodes else [pool.next()]
        for node in nodes:
            run = self.runs.build_new_run(self, run_time, node, manual=manual)
            self.watch(run)
            event.EventManager.get_instance().add(run, parent=self)
            self.notify(self.EVENT_RUN_CREATED)
            yield run

    def handle_job_run_state_change(self, job_run, event):
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
        attrs = [
                'name',
                'queueing',
                'scheduler',
                'node_pool',
                'all_nodes',
                'action_graph',
                'enabled',
                'output_path',
        ]
        return all(
            getattr(other, attr, None) == getattr(self, attr, None)
            for attr in attrs
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
        self.job                = job
        self.shutdown_requested = False
        self.watch(job)

    def restore_job_state(self, job_state_data):
        """Restore the job state and schedule any JobRuns."""
        self.job.restore_state(job_state_data)
        scheduled = self.job.runs.get_scheduled()
        for job_run in scheduled:
            self._set_callback(job_run)

    def enable(self):
        """Enable the job and start its scheduling cycle."""
        self.job.enabled = True
        for job_run in self.get_runs_to_schedule(ignore_last_run_time=True):
            self._set_callback(job_run)

    def disable(self):
        """Disable the job and cancel and pending scheduled jobs."""
        self.job.enabled = False
        self.job.runs.cancel_pending()

    @property
    def is_shutdown(self):
        """Return True if there are no running or starting runs."""
        return not any(self.job.runs.get_active())

    def manual_start(self, run_time=None):
        """Trigger a job run manually (instead of from the scheduler)."""
        run_time = run_time or timeutils.current_time()
        manual_runs = list(self.job.build_new_runs(run_time, manual=True))
        for r in manual_runs:
            r.start()
        return manual_runs

    def schedule_reconfigured(self):
        """Remove the pending run and create new runs with the new JobScheduler.
        """
        self.job.runs.remove_pending()
        self.schedule()

    def schedule(self):
        """Schedule the next run for this job by setting a callback to fire
        at the appropriate time.
        """
        if not self.job.enabled:
            return

        for job_run in self.get_runs_to_schedule():
            self._set_callback(job_run)

    def _set_callback(self, job_run):
        """Set a callback for JobRun to fire at the appropriate time."""
        log.info("Scheduling next Jobrun for %s", self.job.name)
        seconds = job_run.seconds_until_run_time()
        reactor.callLater(seconds, self.run_job, job_run)

    def run_job(self, job_run, run_queued=False):
        """Triggered by a callback to actually start the JobRun. Also
        schedules the next JobRun.
        """
        if self.shutdown_requested:
            return

        # If the Job has been disabled after this run was scheduled, then cancel
        # the JobRun and do not schedule another
        if not self.job.enabled:
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

        node = job_run.node if self.job.all_nodes else None
        # If there is another job run still running, queue or cancel this one
        if any(self.job.runs.get_active(node)):
            if self.job.queueing:
                log.info("%s still running, queueing %s." % (self.job, job_run))
                return job_run.queue()

            log.info("%s still running, cancelling %s." % (self.job, job_run))
            return job_run.cancel()

        job_run.start()
        self.schedule()

    def handle_job_events(self, _observable, event):
        """Handle notifications from observables. If a JobRun has completed
        look for queued JobRuns that may need to start now.
        """
        if event != Job.NOTIFY_RUN_DONE:
            return

        # TODO: this should only start runs on the same node if this is an
        # all_nodes job, but that is currently not possible
        queued_run = self.job.runs.get_first_queued()
        if queued_run:
            reactor.callLater(0, self.run_job, queued_run, run_queued=True)
    handler = handle_job_events

    def get_runs_to_schedule(self, ignore_last_run_time=False):
        """If the scheduler does not support queuing overlapping and this job
        has queued runs, do not schedule any more yet. Otherwise schedule
        the next run.
        """
        queue_overlapping = self.job.scheduler.queue_overlapping

        if not queue_overlapping and self.job.runs.has_pending:
            log.info("%s has pending runs, can't schedule more." % self.job)
            return []

        if ignore_last_run_time:
            last_run_time = None
        else:
            last_run = self.job.runs.get_newest(include_manual=False)
            last_run_time = last_run.run_time if last_run else None
        next_run_time = self.job.scheduler.next_run_time(last_run_time)
        return self.job.build_new_runs(next_run_time)