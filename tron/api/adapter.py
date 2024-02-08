"""
 Classes which create external representations of core objects. This allows
 the core objects to remain decoupled from the API and clients. These classes
 act as an adapter between the data format api clients expect, and the internal
 data of an object.
"""
import functools
import os.path
import time
from typing import List
from urllib.parse import quote

from tron import actioncommand
from tron import scheduler
from tron.core.actionrun import KubernetesActionRun
from tron.serialize import filehandler
from tron.utils import timeutils
from tron.utils.scribereader import read_log_stream_for_action_run
from tron.utils.timeutils import delta_total_seconds


class ReprAdapter:
    """Creates a dictionary from the given object for a set of rules."""

    field_names = []
    translated_field_names = []

    def __init__(self, internal_obj):
        self._obj = internal_obj
        self.fields = self._get_field_names()
        self.translators = self._get_translation_mapping()

    def _get_field_names(self):
        return self.field_names

    def _get_translation_mapping(self):
        return {field_name: getattr(self, "get_%s" % field_name) for field_name in self.translated_field_names}

    def get_repr(self):
        repr_data = {field: getattr(self._obj, field) for field in self.fields}
        translated = {field: func() for field, func in self.translators.items()}
        repr_data.update(translated)
        return repr_data


def adapt_many(adapter_class, seq, *args, **kwargs):
    return [adapter_class(item, *args, **kwargs).get_repr() for item in seq if item is not None]


def toggle_flag(flag_name):
    """Create a decorator which checks if flag_name is true before running
    the wrapped function. If False returns None.
    """

    def wrap(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            if getattr(self, flag_name):
                return f(self, *args, **kwargs)
            return None

        return wrapper

    return wrap


class RunAdapter(ReprAdapter):
    """Base class for JobRun and ActionRun adapters."""

    def get_state(self):
        return self._obj.state

    def get_node(self):
        return NodeAdapter(self._obj.node).get_repr()

    def get_duration(self):
        duration = timeutils.duration(self._obj.start_time, self._obj.end_time)
        return str(duration or "")


class ActionRunAdapter(RunAdapter):
    """Adapt a JobRun and an Action name to an external representation of an
    ActionRun.
    """

    field_names = [
        "id",
        "start_time",
        "end_time",
        "exit_status",
        "action_name",
        "exit_statuses",
        "retries_remaining",
        "original_command",
    ]

    translated_field_names = [
        "state",
        "node",
        "command",
        "raw_command",
        "requirements",
        "meta",
        "stdout",
        "stderr",
        "duration",
        "job_name",
        "run_num",
        "retries_delay",
        "in_delay",
        "triggered_by",
        "trigger_downstreams",
    ]

    def __init__(
        self,
        action_run,
        job_run=None,
        max_lines=10,
        include_stdout=False,
        include_stderr=False,
        include_meta=False,
    ):
        super().__init__(action_run)
        self.job_run = job_run
        self.max_lines = max_lines or None
        self.include_stdout = include_stdout
        self.include_stderr = include_stderr
        self.include_meta = include_meta

    def get_raw_command(self):
        return self._obj.command_config.command

    def get_command(self):
        return self._obj.rendered_command

    @toggle_flag("job_run")
    def get_requirements(self):
        action_name = self._obj.action_name
        required = self.job_run.action_graph.get_dependencies(action_name)
        return [act.name for act in required]

    def _get_serializer(self, path=None) -> filehandler.OutputStreamSerializer:
        path = filehandler.OutputPath(path) if path else self._obj.output_path
        return filehandler.OutputStreamSerializer(path)

    def _get_alternate_output_paths(self):
        try:
            namespace, jobname, run_num, action = self._obj.id.split(".")
        except Exception:
            return None

        # Check to see if the output might have ended up in any alternate locations.
        for alt_path in self._obj.STDOUT_PATHS:
            formatted_alt_path = os.path.join(
                # This ugliness is getting the "root output directory"
                self._obj.context.next.next.base.job.output_path.base,
                alt_path.format(
                    namespace=namespace,
                    jobname=jobname,
                    run_num=run_num,
                    action=action,
                ),
            )
            if os.path.exists(formatted_alt_path):
                yield formatted_alt_path

    @toggle_flag("include_meta")
    def get_meta(self) -> List[str]:
        if not isinstance(self._obj, KubernetesActionRun):
            return ["When this action is migrated to Kubernetes, this will contain Tron/task_processing output."]

        # We're reusing the "old" (i.e., SSH/Mesos) logging files for task_processing output since
        # that won't make it into anything but Splunk
        filename = actioncommand.ActionCommand.STDERR
        output: List[str] = self._get_serializer().tail(filename, self.max_lines)
        if not output:
            for alt_path in self._get_alternate_output_paths():
                output = self._get_serializer(alt_path).tail(filename, self.max_lines)
                if output:
                    return output
        return output

    @toggle_flag("include_stdout")
    def get_stdout(self) -> List[str]:
        if isinstance(self._obj, KubernetesActionRun):
            # it's possible that we have a job that logs to the samestream as another job on a
            # different master (e.g., 1 job in pnw-devc and another in norcal-devc), so we
            # additionally filter by the cluster in each log message.
            # we get this information from the last attempt for this ActionRun, but
            # all of the attempts should always have the same value. This value is guaranteed
            # to be here as it's part of the PaaSTA Contract, but there's also a fallback in
            # read_log_stream_for_action_run() to use the current superregion for the tron
            # master should something go horribly wrong
            paasta_cluster = None
            if self._obj.attempts:
                paasta_cluster = self._obj.attempts[-1].command_config.env.get("PAASTA_CLUSTER")

            return read_log_stream_for_action_run(
                action_run_id=self._obj.id,
                component="stdout",
                # we update the start time of an ActionRun on a retry so we can't just use
                # that start time to figure out when we should start displaying logs for.
                # instead, we use the first attempt's start time as the date from which to
                # start getting logs from and the last attempt's end time as the date at
                # which we stop getting logs from.
                # in the case of an action that completed on its initial run, there will
                # only be one attempt, but that's fine as these single attempts will still
                # have the correct information.
                # XXX: this is suboptimal if there's many days between retries
                min_date=self._obj.attempts[0].start_time if self._obj.attempts else None,
                max_date=self._obj.attempts[-1].end_time if self._obj.attempts else None,
                paasta_cluster=paasta_cluster,
                max_lines=self.max_lines,
            )

        filename = actioncommand.ActionCommand.STDOUT
        output = self._get_serializer().tail(filename, self.max_lines)
        if not output:
            for alt_path in self._get_alternate_output_paths():
                output = self._get_serializer(alt_path).tail(filename, self.max_lines)
                if output:
                    break
        return output

    @toggle_flag("include_stderr")
    def get_stderr(self) -> List[str]:
        if isinstance(self._obj, KubernetesActionRun):
            # it's possible that we have a job that logs to the samestream as another job on a
            # different master (e.g., 1 job in pnw-devc and another in norcal-devc), so we
            # additionally filter by the cluster in each log message.
            # we get this information from the last attempt for this ActionRun, but
            # all of the attempts should always have the same value. This value is guaranteed
            # to be here as it's part of the PaaSTA Contract, but there's also a fallback in
            # read_log_stream_for_action_run() to use the current superregion for the tron
            # master should something go horribly wrong
            paasta_cluster = None
            if self._obj.attempts:
                paasta_cluster = self._obj.attempts[-1].command_config.env.get("PAASTA_CLUSTER")

            return read_log_stream_for_action_run(
                action_run_id=self._obj.id,
                component="stderr",
                # we update the start time of an ActionRun on a retry so we can't just use
                # that start time to figure out when we should start displaying logs for.
                # instead, we use the first attempt's start time as the date from which to
                # start getting logs from and the last attempt's end time as the date at
                # which we stop getting logs from.
                # in the case of an action that completed on its initial run, there will
                # only be one attempt, but that's fine as these single attempts will still
                # have the correct information.
                # XXX: this is suboptimal if there's many days between retries
                min_date=self._obj.attempts[0].start_time if self._obj.attempts else None,
                max_date=self._obj.attempts[-1].end_time if self._obj.attempts else None,
                paasta_cluster=paasta_cluster,
                max_lines=self.max_lines,
            )

        filename = actioncommand.ActionCommand.STDERR
        output = self._get_serializer().tail(filename, self.max_lines)
        if not output:
            for alt_path in self._get_alternate_output_paths():
                output = self._get_serializer(alt_path).tail(filename, self.max_lines)
                if output:
                    break
        return output

    def get_job_name(self):
        return self._obj.job_run_id.rsplit(".", 1)[-2]

    def get_run_num(self):
        return self._obj.job_run_id.split(".")[-1]

    def get_retries_delay(self):
        if self._obj.retries_delay:
            return str(self._obj.retries_delay)

    def get_in_delay(self):
        if self._obj.in_delay is not None:
            return self._obj.in_delay.getTime() - time.time()

    def get_triggered_by(self) -> str:
        remaining = set(self._obj.remaining_triggers)
        all_triggers = sorted(self._obj.rendered_triggers)
        return ", ".join(f"{trig}{' (done)' if trig not in remaining else ''}" for trig in all_triggers)

    def get_trigger_downstreams(self) -> str:
        triggers_to_emit = self._obj.triggers_to_emit()
        return ", ".join(sorted(triggers_to_emit))


class ActionGraphAdapter:
    def __init__(self, action_graph):
        self.action_graph = action_graph

    def get_repr(self):
        def build(action_name):
            action = self.action_graph[action_name]
            dependencies = self.action_graph.get_dependencies(action_name, include_triggers=True)
            return {
                "name": action.name,
                "command": action.command,
                "dependencies": [d.name for d in dependencies],
            }

        return [build(action) for action in self.action_graph.names(include_triggers=True)]


class ActionRunGraphAdapter:
    def __init__(self, action_run_collection):
        self.action_runs = action_run_collection

    def get_repr(self):
        def build(action_run):
            graph = self.action_runs.action_graph
            dependencies = graph.get_dependencies(action_run.action_name, include_triggers=True)
            return {
                "id": action_run.id,
                "name": action_run.action_name,
                "command": action_run.rendered_command,
                "raw_command": action_run.command_config.command,
                "state": action_run.state,
                "start_time": action_run.start_time,
                "end_time": action_run.end_time,
                "dependencies": [d.name for d in dependencies],
            }

        def build_trigger(trigger_name):
            graph = self.action_runs.action_graph
            trigger = graph[trigger_name]
            dependencies = graph.get_dependencies(trigger_name, include_triggers=True)
            return {
                "name": trigger.name,
                "command": trigger.command,
                "dependencies": [d.name for d in dependencies],
                "state": "unknown",
            }

        return [build(action_run) for action_run in self.action_runs] + [
            build_trigger(trigger_name) for trigger_name in self.action_runs.action_graph.all_triggers
        ]


class JobRunAdapter(RunAdapter):

    field_names = [
        "id",
        "run_num",
        "run_time",
        "start_time",
        "end_time",
        "manual",
        "job_name",
    ]
    translated_field_names = [
        "state",
        "node",
        "duration",
        "url",
        "runs",
        "action_graph",
    ]

    def __init__(
        self,
        job_run,
        include_action_runs=False,
        include_action_graph=False,
    ):
        super().__init__(job_run)
        self.include_action_runs = include_action_runs
        self.include_action_graph = include_action_graph

    def get_url(self):
        return f"/jobs/{self._obj.job_name}/{self._obj.run_num}"

    @toggle_flag("include_action_runs")
    def get_runs(self):
        return adapt_many(ActionRunAdapter, self._obj.action_runs, self._obj)

    @toggle_flag("include_action_graph")
    def get_action_graph(self):
        return ActionRunGraphAdapter(self._obj.action_runs).get_repr()


class JobAdapter(ReprAdapter):

    field_names = ["status", "all_nodes", "allow_overlap", "queueing"]
    translated_field_names = [
        "name",
        "scheduler",
        "action_names",
        "node_pool",
        "last_success",
        "next_run",
        "url",
        "runs",
        "max_runtime",
        "action_graph",
        "monitoring",
        "expected_runtime",
        "actions_expected_runtime",
    ]

    def __init__(
        self,
        job,
        include_job_runs=False,
        include_action_runs=False,
        include_action_graph=True,
        include_node_pool=True,
        num_runs=None,
    ):
        super().__init__(job)
        self.include_job_runs = include_job_runs
        self.include_action_runs = include_action_runs
        self.include_action_graph = include_action_graph
        self.include_node_pool = include_node_pool
        self.num_runs = num_runs

    def get_name(self):
        return self._obj.get_name()

    def get_monitoring(self):
        return self._obj.get_monitoring()

    def get_scheduler(self):
        return SchedulerAdapter(self._obj.scheduler).get_repr()

    def get_action_names(self):
        return list(self._obj.action_graph.names())

    @toggle_flag("include_node_pool")
    def get_node_pool(self):
        return NodePoolAdapter(self._obj.node_pool).get_repr()

    def get_last_success(self):
        last_success = self._obj.runs.last_success
        return last_success.end_time if last_success else None

    def get_next_run(self):
        next_run = self._obj.runs.next_run
        return next_run.run_time if next_run else None

    def get_url(self):
        return f"/jobs/{quote(self._obj.get_name())}"

    @toggle_flag("include_job_runs")
    def get_runs(self):
        runs = adapt_many(
            JobRunAdapter,
            list(self._obj.runs)[: self.num_runs or None],
            self.include_action_runs,
        )
        return runs

    def get_max_runtime(self):
        return str(self._obj.max_runtime)

    def get_expected_runtime(self):
        return delta_total_seconds(self._obj.expected_runtime)

    def get_actions_expected_runtime(self):
        return self._obj.action_graph.expected_runtime

    @toggle_flag("include_action_graph")
    def get_action_graph(self):
        return ActionGraphAdapter(self._obj.action_graph).get_repr()


class JobIndexAdapter(ReprAdapter):

    translated_field_names = ["name", "actions"]

    def get_name(self):
        return self._obj.get_name()

    def get_actions(self):
        def adapt_run(run):
            return {"name": run.action_name, "command": run.command_config.command}

        job_run = self._obj.get_runs().get_newest()
        if not job_run:
            return []
        return [adapt_run(action_run) for action_run in job_run.action_runs]


class SchedulerAdapter(ReprAdapter):

    translated_field_names = ["value", "type", "jitter"]

    def get_value(self):
        return self._obj.get_value()

    def get_type(self):
        return self._obj.get_name()

    def get_jitter(self):
        return scheduler.get_jitter_str(self._obj.get_jitter())


class EventAdapter(ReprAdapter):

    field_names = ["name", "entity", "time"]
    translated_field_names = ["level"]

    def get_level(self):
        return self._obj.level.label


class NodeAdapter(ReprAdapter):
    field_names = ["name", "hostname", "username", "port"]


class NodePoolAdapter(ReprAdapter):
    translated_field_names = ["name", "nodes"]

    def get_name(self):
        return self._obj.get_name()

    def get_nodes(self):
        return adapt_many(NodeAdapter, self._obj.get_nodes())
