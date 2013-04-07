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


def adapt_many(adapter_class, seq, *args):
    return [adapter_class(item, *args).get_repr() for item in seq]


class RunAdapter(ReprAdapter):
    """Base class for JobRun and ActionRun adapters."""

    def get_state(self):
        return self._obj.state.name

    def get_node(self):
        return str(self._obj.node)

    def get_duration(self):
        duration = timeutils.duration(self._obj.start_time, self._obj.end_time)
        return str(duration or '')


class ActionRunAdapter(RunAdapter):
    """Adapt a JobRun and an Action name to an external representation of an
    ActionRun.
    """

    field_names = [
            'id',
            'start_time',
            'end_time',
            'exit_status',
            'action_name'
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

    def __init__(self, action_run, job_run, max_lines=10):
        super(ActionRunAdapter, self).__init__(action_run)
        self.job_run            = job_run
        self.max_lines          = max_lines

    def get_raw_command(self):
        return self._obj.bare_command

    def get_command(self):
        return self._obj.rendered_command

    def get_requirements(self):
        action_name = self._obj.action_name
        required = self.job_run.action_graph.get_required_actions(action_name)
        return [act.name for act in required]

    def _get_serializer(self):
        return filehandler.OutputStreamSerializer(self._obj.output_path)

    def get_stdout(self):
        filename = actioncommand.ActionCommand.STDOUT
        return self._get_serializer().tail(filename, self.max_lines)

    def get_stderr(self):
        filename = actioncommand.ActionCommand.STDERR
        return self._get_serializer().tail(filename, self.max_lines)


class ActionGraphAdapter(object):

    def __init__(self, action_graph):
        self.action_graph = action_graph

    def get_repr(self):
        def build(action):
            return {
                'name':         action.name,
                'command':      action.command,
                'dependent':    [dep.name for dep in action.dependent_actions],
            }

        return [build(action) for action in self.action_graph.get_actions()]

class ActionRunGraphAdapter(object):

    def __init__(self, action_run_collection):
        self.action_runs = action_run_collection

    def get_repr(self):
        def build(action_run):
            deps = self.action_runs.action_graph.get_dependent_actions(
                action_run.action_name)
            return {
                'id':           action_run.id,
                'name':         action_run.action_name,
                'command':      action_run.rendered_command,
                'raw_command':  action_run.bare_command,
                'state':        action_run.state.name,
                'start_time':   action_run.start_time,
                'end_time':     action_run.end_time,
                'dependent':    [dep.name for dep in deps],
            }

        return [build(action_run) for action_run in self.action_runs]


class JobRunAdapter(RunAdapter):

    field_names = [
       'id',
        'run_num',
        'run_time',
        'start_time',
        'end_time',
        'manual',
        'job_name',
    ]
    translated_field_names = [
        'state',
        'node',
        'duration',
        'url',
        'runs',
        'action_graph',
    ]

    def __init__(self, job_run,
            include_action_runs=False,
            include_action_graph=False):
        super(JobRunAdapter, self).__init__(job_run)
        self.include_action_runs = include_action_runs
        self.include_action_graph = include_action_graph

    def get_url(self):
        return '/jobs/%s/%s' % (self._obj.job_name, self._obj.run_num)

    def get_runs(self):
        if not self.include_action_runs:
            return
        return adapt_many(ActionRunAdapter, self._obj.action_runs, self._obj)

    # TODO: convert to decorator
    def get_action_graph(self):
        if self.include_action_graph:
            return ActionRunGraphAdapter(self._obj.action_runs).get_repr()

class JobAdapter(ReprAdapter):

    field_names = ['name', 'status', 'all_nodes', 'allow_overlap', 'queueing']
    translated_field_names = [
        'scheduler',
        'action_names',
        'node_pool',
        'last_success',
        'next_run',
        'url',
        'runs',
        'action_graph',
    ]

    def __init__(self, job,
             include_job_runs=False,
             include_action_runs=False,
             num_runs=None):
        super(JobAdapter, self).__init__(job)
        self.include_job_runs    = include_job_runs
        self.include_action_runs = include_action_runs
        self.num_runs            = num_runs

    def get_scheduler(self):
        return str(self._obj.scheduler)

    def get_action_names(self):
        return self._obj.action_graph.names

    def get_node_pool(self):
        return self._obj.node_pool.get_name()

    def get_last_success(self):
        last_success = self._obj.runs.last_success
        return last_success.end_time if last_success else None

    def get_next_run(self):
        next_run = self._obj.runs.next_run
        return next_run.run_time if next_run else None

    def get_url(self):
        return '/jobs/%s' % urllib.quote(self._obj.name)

    def get_runs(self):
        if not self.include_job_runs:
            return
        runs = adapt_many(JobRunAdapter, self._obj.runs, self.include_action_runs)
        return runs[:self.num_runs or None]

    # TODO: create a flag to include/exclude this
    def get_action_graph(self):
        return ActionGraphAdapter(self._obj.action_graph).get_repr()


class ServiceAdapter(ReprAdapter):

    field_names = ['name', 'enabled']
    translated_field_names = [
        'count',
        'url',
        'state',
        'command',
        'pid_filename',
        'instances',
        'node_pool',
        'live_count',
        'monitor_interval',
        'restart_interval',
        'events']

    def __init__(self, service, include_events=False):
        super(ServiceAdapter, self).__init__(service)
        self.include_events = include_events

    def get_url(self):
        return "/services/%s" % urllib.quote(self._obj.get_name())

    def get_count(self):
        return self._obj.config.count

    def get_state(self):
        return self._obj.get_state()

    def get_command(self):
        return self._obj.config.command

    def get_pid_filename(self):
        return self._obj.config.pid_file

    def get_instances(self):
        return adapt_many(ServiceInstanceAdapter, self._obj.instances)

    def get_node_pool(self):
        return self._obj.config.node

    def get_live_count(self):
        return len(self._obj.instances)

    def get_monitor_interval(self):
        return self._obj.config.monitor_interval

    def get_restart_interval(self):
        return self._obj.config.restart_interval

    # TODO: use decorator
    def get_events(self):
        if self.include_events:
            events = adapt_many(EventAdapter, self._obj.event_recorder.list())
            return events[:self.include_events]

class ServiceInstanceAdapter(ReprAdapter):

    field_names = ['id', 'failures']
    translated_field_names = ['state', 'node']

    def get_state(self):
        return str(self._obj.get_state())

    def get_node(self):
        return str(self._obj.node)


class EventAdapter(ReprAdapter):

    field_names = ['name', 'entity', 'time']
    translated_field_names = ['level']

    def get_level(self):
        return self._obj.level.label
