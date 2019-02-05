import logging

import humanize
from twisted.internet import reactor

from tron.core import recovery
from tron.core.job import Job
from tron.scheduler import scheduler_from_config
from tron.serialize import filehandler
from tron.utils import timeutils
from tron.utils.observer import Observer

log = logging.getLogger(__name__)


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
        if self.job.enabled:
            self.job.runs.remove_pending()
            self.create_and_schedule_runs(ignore_last_run_time=True)

    def schedule(self):
        """Schedule the next run for this job by setting a callback to fire
        at the appropriate time.
        """
        if not self.job.enabled:
            return
        self.create_and_schedule_runs()

    def update_from_job_scheduler(self, job_scheduler):
        """ Update a job scheduler by copying another. """
        curr_job = self.get_job()
        new_job = job_scheduler.get_job()

        curr_job.update_from_job(new_job)

        # Since job updating only copies equality attributes (defined in the Job
        # class), we need to now enable or disable the job depending on if the
        # new job says so.
        if (curr_job.enabled is not new_job.enabled and
                curr_job.config_enabled is not new_job.config_enabled):
            if new_job.config_enabled:
                log.info(f'{curr_job} re-enabled during reconfiguration')
                self.enable()
            else:
                log.info(f'{curr_job} disabled during reconfiguration')
                self.disable()
        curr_job.config_enabled = new_job.config_enabled

    def _set_callback(self, job_run):
        """Set a callback for JobRun to fire at the appropriate time."""
        seconds = job_run.seconds_until_run_time()
        human_time = humanize.naturaltime(seconds, future=True)
        log.info(f"Scheduling {job_run} {human_time} ({seconds} seconds)")
        reactor.callLater(seconds, self.run_job, job_run)

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
            reactor.callLater(seconds, job_run.stop)

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
            reactor.callLater(0, self.run_job, queued_run, run_queued=True)

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

    def update_name(self, name):
        self.job.name = name
        for job_run in self.get_job_runs():
            for action_run in job_run._get_action_runs():
                action_run.job_run_id = action_run.job_run_id.replace(job_run.job_name, name, 1)
            job_run.job_name = name

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

    def __init__(self, context, output_stream_dir, time_zone, action_runner, job_graph):
        self.context = context
        self.output_stream_dir = output_stream_dir
        self.time_zone = time_zone
        self.action_runner = action_runner
        self.job_graph = job_graph

    def build(self, job_config):
        log.debug(f"Building new job {job_config.name}")
        output_path = filehandler.OutputPath(self.output_stream_dir)
        time_zone = job_config.time_zone or self.time_zone
        scheduler = scheduler_from_config(job_config.schedule, time_zone)
        action_graph = self.job_graph.get_action_graph_for_job(job_config.name)
        job = Job.from_config(
            job_config=job_config,
            scheduler=scheduler,
            parent_context=self.context,
            output_path=output_path,
            action_runner=self.action_runner,
            action_graph=action_graph,
        )
        return JobScheduler(job)
