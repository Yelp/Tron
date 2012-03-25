import logging

from tron.core.actionrun import ActionRun

log = logging.getLogger('tron.action')


class Action(object):
    """A configurable data object which stores data about an action. Used as a
    node in an `tron.core.actiongraph.ActionGraph`
    """

    def __init__(self, name=None, command=None, node_pool=None,
                 required_actions=None):
        self.name = name
        self.command = command
        self.node_pool = node_pool

        self.required_actions = required_actions or []

    @classmethod
    def from_config(cls, config, node_pools):
        return cls(
            name=config.name,
            command=config.command,
            node_pool=node_pools[config.node] if config.node else None
        )

    def __eq__(self, other):
        if (not isinstance(other, Action)
           or self.name != other.name
           or self.command != other.command):
            return False

        return all(me == you for (me, you) in zip(self.required_actions,
                                                   other.required_actions))

    def __ne__(self, other):
        return not self == other

    def build_run(self, job_run, cleanup=False):
        """Build an instance of ActionRun for this action."""
        if cleanup:
            callback = job_run.notify_cleanup_action_run_completed
        else:
            callback = job_run.notify_action_run_completed

        action_run = ActionRun(
            self,
            context=job_run.context,
            node=job_run.node,
            id="%s.%s" % (job_run.id, self.name),
            output_path=job_run.output_path,
            run_time=job_run.run_time)

        # TODO: these should now be setup using watch() on Jobrun.for_job
        # but I need to add the watcher

        # Notify on any state change so state can be serialized
        action_run.machine.listen(True, job_run.job.notify_state_changed)
        # Notify when we reach an end state so the next run can be scheduled
        action_run.machine.listen(ActionRun.END_STATES, callback)
        return action_run
