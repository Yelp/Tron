"""
 tron.core.jobrun
"""

from collections import deque
import logging
from tron.action import ActionRun
from tron.utils.observer import Observable, Observer

log = logging.getLogger('tron.core.jobrun')

class JobRun(Observable, Observer):

    def __init__(self, id, run_num, run_time, node, base_path, context=None,
         action_runs=None
    ):
        super(JobRun, self).__init__()

    @classmethod
    def from_state(cls, state, actions):
        pass

    @classmethod
    def for_job(cls, job, run_num, run_time, node):
        """Create a JobRun for a job."""
        id = "%s.%s" % (job.name, run_num)
        run = cls(id, run_num, run_time, node,
            job.output_path, job.context,
        )

        run.register_cleanup_action(job.cleanup_action.build_run(run))
        run.register_action_runs(job.action_graph.build_action_run(run))
        return run

    def register_action_runs(self, action_runs):
        """Store action runs and register callbacks."""
        self.action_runs = action_runs
        for action_run in action_runs:
            self.watch(action_run, True)

    def register_cleanup_action(self, cleanup_action):
        pass



class JobRunCollection(object):
    """A JobRunCollection is a deque of JobRun objects. Responsible for
    ordering and logic related to a group of JobRuns which should all be runs
    for the same Job.

    A JobRunCollection is created in two stages. First it's populated from a
    configuration object, and second its state is loaded from a serialized
    state dict.

    Runs in a JobRunCollection should always remain sorted by their run_num.
    """

    def __init__(self, run_limit):
        self.run_limit = run_limit
        self.runs = deque()

    @classmethod
    def from_config(cls, job_config):
        """Factory method for creating a JobRunCollection from a config."""
        return cls(job_config.run_limit)

    def restore_state(self, state, actions):
        """Apply state to all jobs from the state dict."""
        for run_state in state:
            # TODO: test this is added in the correct order
            self.runs.appendleft(JobRun.from_state(run_state, actions))

    def build_new_run(self, job, run_time, node):
        """Create a new run for the job, add it to the runs list,
        and return it.
        """
        run_num = self.next_run_num()
        log.info("Building JobRun %s for %s on %s at %s" %
             (run_num, job, node, run_time))

        run = JobRun.for_job(job, run_num, run_time, node)
        self.runs.appendleft(run)
        self.remove_old_runs()
        return run

    def cancel_pending(self):
        """Find any queued or scheduled runs and cancel them."""
        pending_func = lambda r: r.is_scheduled or r.is_queued
        pending = self._get_run_using(pending_func)
        while pending:
            pending.cancel()
            pending = self._get_run_using(pending_func)

    def _get_run_using(self, func):
        """Find the first run (from most recent to least recent), where func()
        returns true.  func() should be a callable which tables a single
        argument (a JobRun), and return True or False.
        """
        for run in self.runs:
            if func(run):
                return run
        return None

    def get_run_by_state(self, state):
        """Returns the most recent run which matches the state."""
        return self._get_run_using(lambda r: r.state == state)

    def get_run_by_num(self, num):
        """Return a the run with run number which matches num."""
        return self._get_run_using(lambda r: r.run_num == num)

    def get_run_by_state_short_name(self, short_name):
        """Returns the most recent run which matches the state short name."""
        return self._get_run_using(lambda r: r.state.short_name == short_name)

    def get_newest(self):
        """Returns the most recently created JobRun."""
        return self.runs[0] if self.runs else None

    def get_next_to_finish(self, node=None):
        """Return the most recent run which is either running or scheduled. If
        node is none None, then only looks for runs on that node.
        """
        def compare(run):
            if node and run.node != node:
                return False
            if run.is_running or run.is_scheduled:
                return run
        return self._get_run_using(compare)

    def next_run_num(self):
        """Return the next run number to use."""
        return max(r.run_num for r in self.runs) + 1

    def remove_old_runs(self):
        """Remove old runs to attempt to reduce the number of completed runs
        to within RUN_LIMIT.
        """
        next = self.get_next_to_finish()
        next_num = next.run_num if next else self.runs[0].run_num
        last_success = self.get_run_by_state(ActionRun.STATE_SUCCEEDED)
        succ_num = last_success.run_num if last_success else 0
        keep_num = min(next_num, succ_num)

        while (
            len(self.runs) > self.run_limit and
            keep_num > self.runs[-1].run_num
        ):
            run = self.runs.pop()
            run.cleanup()

    @property
    def state_data(self):
        """Return the state data to serialize."""
        return [r.state_data for r in self.runs]