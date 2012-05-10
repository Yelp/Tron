"""
 Classes which create external representations of core objects. This allows
 the core objects to remain decoupled from the API and clients. These classes
 act as an adapter between the data format api clients expect, and the internal
 data of an object.
"""
import urllib
from tron import actioncommand
from tron.serialize import filehandler
from tron.utils import timeutils

class ReprAdapter(object):
    """Creates a dictionary from the given object for a set of rules."""

    field_names = []
    translated_field_names = []

    def __init__(self, internal_obj):
        self._obj               = internal_obj
        self.fields             = self._get_field_names()
        self.translators        = self._get_translation_mapping()

    def _get_field_names(self):
        return self.field_names

    def _get_translation_mapping(self):
        return dict(
            (field_name, getattr(self, 'get_%s' % field_name))
            for field_name in self.translated_field_names)

    def get_repr(self):
        repr_data = dict(
                (field, getattr(self._obj, field)) for field in self.fields)
        translated = dict(
                (field, func()) for field, func in self.translators.iteritems())
        repr_data.update(translated)
        return repr_data


class RunAdapter(ReprAdapter):
    """Base class for JobRun and ActionRun adapters."""

    def get_state(self):
        return self._obj.state.short_name

    def get_node(self):
        node = self._obj.node
        return node.hostname if node else None

    def get_duration(self):
        duration = timeutils.duration(self._obj.start_time, self._obj.end_time)
        return str(duration or '')


class ActionRunAdapter(RunAdapter):
    """Adapt a JobRun and an Action name to an external representation of an
    ActionRun.
    """

    field_names = [
            'id',
            'job_run_time',
            'start_time',
            'end_time',
            'exit_status'
    ]

    translated_field_names = [
            'state',
            'node',
            'command',
            'raw_command',
            'requirements',
            'stdout',
            'stderr',
            'duration'
    ]

    def __init__(self, job_run, action_name, max_lines=10):
        action_run = job_run.action_runs[action_name]
        super(ActionRunAdapter, self).__init__(action_run)
        self.job_run            = job_run
        self.max_lines          = max_lines
        self.serializer         = filehandler.OutputStreamSerializer(
                                    action_run.output_path)

    def get_raw_command(self):
        return self._obj.bare_command

    def get_command(self):
        return self._obj.rendered_command or self._obj.bare_command

    def get_requirements(self):
        action_name = self._obj.action_name
        required = self.job_run.action_graph.get_required_actions(action_name)
        return [act.name for act in required]

    def get_stdout(self):
        filename = actioncommand.ActionCommand.STDOUT
        return self.serializer.tail(filename, self.max_lines)

    def get_stderr(self):
        filename = actioncommand.ActionCommand.STDERR
        return self.serializer.tail(filename, self.max_lines)


class JobRunAdapter(RunAdapter):

    field_names = [
            'id', 'run_num', 'run_time', 'start_time', 'end_time', 'manual']
    translated_field_names = ['state', 'node', 'duration', 'href', 'runs']

    def __init__(self, job_run, include_action_runs=False):
        super(JobRunAdapter, self).__init__(job_run)
        self.include_action_runs = include_action_runs

    def get_href(self):
        return '/jobs/%s/%s' % (self._obj.job_name, self._obj.run_num)

    def get_runs(self):
        if not self.include_action_runs:
            return

        return [
            ActionRunAdapter(self._obj, action_name).get_repr()
            for action_name in self._obj.action_runs.names
        ]

class JobAdapter(ReprAdapter):

    field_names = ['name', 'status']
    translated_field_names = [
        'scheduler',
        'action_names',
        'node_pool',
        'last_success',
        'href',
        'runs'
    ]

    def __init__(self, job, include_job_runs=False, include_action_runs=False):
        super(JobAdapter, self).__init__(job)
        self.include_job_runs    = include_job_runs
        self.include_action_runs = include_action_runs

    def get_scheduler(self):
        return str(self._obj.scheduler)

    def get_action_names(self):
        return self._obj.action_graph.names

    def get_node_pool(self):
        return [n.hostname for n in self._obj.node_pool.nodes]

    def get_last_success(self):
        last_success = self._obj.runs.last_success
        return last_success.end_time if last_success else None

    def get_href(self):
        return '/jobs/%s' % urllib.quote(self._obj.name)

    def get_runs(self):
        if not self.include_job_runs:
            return

        return [
            JobRunAdapter(job_run, self.include_action_runs).get_repr()
            for job_run in self._obj.runs
        ]
