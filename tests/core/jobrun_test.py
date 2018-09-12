import datetime
from unittest.mock import MagicMock

import mock
import pytz

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import setup
from testifycompat import TestCase
from tests.assertions import assert_call
from tests.assertions import assert_length
from tests.assertions import assert_raises
from tests.testingutils import autospec_method
from tron import actioncommand
from tron import node
from tron.core import actiongraph
from tron.core import actionrun
from tron.core import job
from tron.core import jobrun
from tron.serialize import filehandler


def build_mock_job():
    action_graph = mock.create_autospec(actiongraph.ActionGraph)
    runner = mock.create_autospec(actioncommand.SubprocessActionRunnerFactory)
    return mock.create_autospec(
        job.Job,
        action_graph=action_graph,
        output_path=mock.Mock(),
        context=mock.Mock(),
        action_runner=runner,
    )


class TestJobRun(TestCase):

    now = datetime.datetime(2012, 3, 14, 15, 9, 20, tzinfo=None)
    now_with_tz = datetime.datetime(2012, 3, 14, 15, 9, 20, tzinfo=pytz.utc)

    @setup
    def setup_jobrun(self):
        self.job = build_mock_job()
        self.action_graph = self.job.action_graph
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        mock_node = mock.create_autospec(node.Node)
        self.job_run = jobrun.JobRun(
            'jobname',
            7,
            self.run_time,
            mock_node,
            action_runs=MagicMock(
                action_runs_with_cleanup=[],
                get_startable_action_runs=lambda: [],
            ),
            eventbus_publish=lambda: None,
        )
        autospec_method(self.job_run.watch)
        autospec_method(self.job_run.notify)
        self.action_run = mock.create_autospec(
            actionrun.ActionRun,
            is_skipped=False,
        )

    def test__init__(self):
        assert_equal(self.job_run.job_name, 'jobname')
        assert_equal(self.job_run.run_time, self.run_time)
        assert str(self.job_run.output_path).endswith(self.job_run.id)

    def test_for_job(self):
        run_num = 6
        mock_node = mock.create_autospec(node.Node)
        run = jobrun.JobRun.for_job(
            self.job,
            run_num,
            self.run_time,
            mock_node,
            False,
            eventbus_publish=lambda: None,
        )

        assert_equal(run.action_runs.action_graph, self.action_graph)
        assert_equal(run.job_name, self.job.get_name.return_value)
        assert_equal(run.run_num, run_num)
        assert_equal(run.node, mock_node)
        assert not run.manual

    def test_for_job_manual(self):
        run_num = 6
        mock_node = mock.create_autospec(node.Node)
        run = jobrun.JobRun.for_job(
            self.job,
            run_num,
            self.run_time,
            mock_node,
            True,
            eventbus_publish=lambda: None,
        )
        assert_equal(run.action_runs.action_graph, self.action_graph)
        assert run.manual

    def test_state_data(self):
        state_data = self.job_run.state_data
        assert_equal(state_data['run_num'], 7)
        assert not state_data['manual']
        assert_equal(state_data['run_time'], self.run_time)

    def test_set_action_runs(self):
        self.job_run._action_runs = None
        count = 2
        action_runs = [
            mock.create_autospec(actionrun.ActionRun) for _ in range(count)
        ]
        run_collection = mock.create_autospec(
            actionrun.ActionRunCollection,
            action_runs_with_cleanup=action_runs,
        )
        self.job_run._set_action_runs(run_collection)
        assert_equal(self.job_run.watch.call_count, count)

        expected = [mock.call(run) for run in action_runs]
        assert_equal(self.job_run.watch.mock_calls, expected)
        assert_equal(self.job_run.action_runs, run_collection)
        assert self.job_run.action_runs_proxy

    def test_set_action_runs_none(self):
        self.job_run._action_runs = None
        run_collection = mock.create_autospec(actionrun.ActionRunCollection)
        self.job_run._set_action_runs(run_collection)
        assert not self.job_run.watch.mock_calls
        assert_equal(self.job_run.action_runs, run_collection)

    def test_set_action_runs_duplicate(self):
        run_collection = mock.create_autospec(actionrun.ActionRunCollection)
        assert_raises(
            ValueError,
            self.job_run._set_action_runs,
            run_collection,
        )

    @mock.patch('tron.core.jobrun.timeutils.current_time', autospec=True)
    def test_seconds_until_run_time(self, mock_current_time):
        mock_current_time.return_value = self.now
        seconds = self.job_run.seconds_until_run_time()
        assert_equal(seconds, 6)

    @mock.patch('tron.core.jobrun.timeutils.current_time', autospec=True)
    def test_seconds_until_run_time_with_tz(self, mock_current_time):
        mock_current_time.return_value = self.now_with_tz
        self.job_run.run_time = self.run_time.replace(tzinfo=pytz.utc)
        seconds = self.job_run.seconds_until_run_time()
        assert_equal(seconds, 6)

    def test_start(self):
        autospec_method(self.job_run._do_start)
        assert self.job_run.start()
        self.job_run._do_start.assert_called_with()

    def test_start_failed(self):
        autospec_method(self.job_run._do_start, return_value=False)
        assert not self.job_run.start()

    def test_start_no_startable_action_runs(self):
        autospec_method(self.job_run._do_start)
        self.job_run.action_runs.has_startable_action_runs = False
        assert not self.job_run.start()

    def test_do_start(self):
        startable_runs = [
            mock.create_autospec(actionrun.ActionRun) for _ in range(3)
        ]
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        assert self.job_run._do_start()
        self.job_run.action_runs.ready.assert_called_with()
        for startable_run in startable_runs:
            startable_run.start.assert_called_with()

    def test_do_start_all_failed(self):
        autospec_method(self.job_run._start_action_runs, return_value=[None])
        assert not self.job_run._do_start()

    def test_do_start_some_failed(self):
        returns = [True, None]
        autospec_method(self.job_run._start_action_runs, return_value=returns)
        assert self.job_run._do_start()

    def test_do_start_no_runs(self):
        assert not self.job_run._do_start()

    def test_start_action_runs(self):
        startable_runs = [
            mock.create_autospec(actionrun.ActionRun) for _ in range(3)
        ]
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        started_runs = self.job_run._start_action_runs()
        assert_equal(started_runs, startable_runs)

    def test_start_action_runs_failed(self):
        startable_runs = [
            mock.create_autospec(actionrun.ActionRun) for _ in range(3)
        ]
        startable_runs[0].start.return_value = False
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        started_runs = self.job_run._start_action_runs()
        assert_equal(started_runs, startable_runs[1:])

    def test_start_action_runs_all_failed(self):
        startable_runs = [
            mock.create_autospec(actionrun.ActionRun) for _ in range(2)
        ]
        for startable_run in startable_runs:
            startable_run.start.return_value = False
        self.job_run.action_runs.get_startable_action_runs = lambda: startable_runs

        started_runs = self.job_run._start_action_runs()
        assert_equal(started_runs, [])

    def test_handler_not_end_state_event(self):
        autospec_method(self.job_run.finalize)
        autospec_method(self.job_run._start_action_runs)
        self.action_run.is_done = False
        self.job_run.handler(self.action_run, mock.Mock())
        assert not self.job_run.finalize.mock_calls
        assert not self.job_run._start_action_runs.mock_calls

    def test_handler_with_startable(self):
        startable_run = mock.create_autospec(actionrun.ActionRun)
        self.job_run.action_runs.get_startable_action_runs = lambda: [
            startable_run, ]
        autospec_method(self.job_run.finalize)
        self.action_run.is_broken = False

        self.job_run.handler(self.action_run, mock.Mock())
        self.job_run.notify.assert_called_with(
            self.job_run.NOTIFY_STATE_CHANGED,
        )
        startable_run.start.assert_called_with()
        assert not self.job_run.finalize.mock_calls

    def test_handler_is_active(self):
        self.job_run.action_runs.is_active = True
        autospec_method(self.job_run._start_action_runs, return_value=[])
        autospec_method(self.job_run.finalize)
        self.job_run.handler(self.action_run, mock.Mock())
        assert not self.job_run.finalize.mock_calls

    def test_handler_finished_without_cleanup(self):
        self.job_run.action_runs.is_active = False
        self.job_run.action_runs.is_scheduled = False
        self.job_run.action_runs.cleanup_action_run = None
        autospec_method(self.job_run.finalize)
        self.job_run.handler(self.action_run, mock.Mock())
        self.job_run.finalize.assert_called_with()

    def test_handler_finished_with_cleanup_done(self):
        self.job_run.action_runs.is_active = False
        self.job_run.action_runs.is_scheduled = False
        self.job_run.action_runs.cleanup_action_run = mock.Mock(is_done=True)
        autospec_method(self.job_run.finalize)
        self.job_run.handler(self.action_run, mock.Mock())
        self.job_run.finalize.assert_called_with()

    def test_handler_finished_with_cleanup(self):
        self.job_run.action_runs.is_active = False
        self.job_run.action_runs.is_scheduled = False
        self.job_run.action_runs.cleanup_action_run = mock.Mock(is_done=False)
        autospec_method(self.job_run.finalize)
        self.job_run.handler(self.action_run, mock.Mock())
        assert not self.job_run.finalize.mock_calls
        self.job_run.action_runs.cleanup_action_run.start.assert_called_with()

    def test_handler_action_run_cancelled(self):
        self.action_run.is_broken = True
        autospec_method(self.job_run._start_action_runs)
        self.job_run.handler(self.action_run, mock.Mock())
        assert not self.job_run._start_action_runs.mock_calls

    def test_handler_action_run_skipped(self):
        self.action_run.is_broken = False
        self.action_run.is_skipped = True
        self.job_run.action_runs.is_scheduled = True
        autospec_method(self.job_run._start_action_runs)
        self.job_run.handler(self.action_run, mock.Mock())
        assert not self.job_run._start_action_runs.mock_calls

    def test_state(self):
        assert_equal(self.job_run.state, actionrun.ActionRun.STATE_SUCCEEDED)

    def test_state_with_no_action_runs(self):
        self.job_run._action_runs = None
        assert_equal(self.job_run.state, actionrun.ActionRun.STATE_UNKNOWN)

    def test_finalize(self):
        self.job_run.action_runs.is_failed = False
        self.job_run.finalize()
        self.job_run.notify.assert_called_with(self.job_run.NOTIFY_DONE)

    def test_finalize_failure(self):
        self.job_run.finalize()
        self.job_run.notify.assert_called_with(self.job_run.NOTIFY_DONE)

    def test_cleanup(self):
        autospec_method(self.job_run.clear_observers)
        self.job_run.output_path = mock.create_autospec(filehandler.OutputPath)
        self.job_run.cleanup()

        self.job_run.clear_observers.assert_called_with()
        self.job_run.output_path.delete.assert_called_with()
        assert not self.job_run.node
        assert not self.job_run.action_graph
        assert not self.job_run.action_runs

    def test__getattr__(self):
        assert self.job_run.cancel
        assert self.job_run.is_queued
        assert self.job_run.is_succeeded

    def test__getattr__miss(self):
        assert_raises(AttributeError, lambda: self.job_run.bogus)


class TestJobRunFromState(TestCase):
    @setup
    def setup_jobrun(self):
        self.action_graph = mock.create_autospec(actiongraph.ActionGraph)
        self.run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        self.path = ['base', 'path']
        self.output_path = mock.create_autospec(filehandler.OutputPath)
        self.node_pool = mock.create_autospec(node.NodePool)
        self.action_run_state_data = [{
            'job_run_id': 'thejobname.22',
            'action_name': 'blingaction',
            'state': 'succeeded',
            'run_time': 'sometime',
            'start_time': 'sometime',
            'end_time': 'sometime',
            'command': 'doit',
            'node_name': 'thenode',
        }]
        self.state_data = {
            'job_name': 'thejobname',
            'run_num': 22,
            'run_time': self.run_time,
            'node_name': 'thebox',
            'end_time': 'the_end',
            'start_time': 'start_time',
            'runs': self.action_run_state_data,
            'cleanup_run': None,
            'manual': True,
        }
        self.context = mock.Mock()

    def test_from_state(self):
        run = jobrun.JobRun.from_state(
            self.state_data,
            self.action_graph,
            self.output_path,
            self.context,
            self.node_pool,
            eventbus_publish=lambda: None,
        )
        assert_length(run.action_runs.run_map, 1)
        assert_equal(run.job_name, self.state_data['job_name'])
        assert_equal(run.run_time, self.run_time)
        assert run.manual
        assert_equal(run.output_path, self.output_path)
        assert run.context.next
        assert run.action_graph

    def test_from_state_node_no_longer_exists(self):
        run = jobrun.JobRun.from_state(
            self.state_data,
            self.action_graph,
            self.output_path,
            self.context,
            self.node_pool,
            eventbus_publish=lambda: None,
        )
        assert_length(run.action_runs.run_map, 1)
        assert_equal(run.job_name, 'thejobname')
        assert_equal(run.run_time, self.run_time)
        assert_equal(run.node, self.node_pool)


class MockJobRun(MagicMock):

    manual = False

    node = 'anode'

    @property
    def is_scheduled(self):
        return self.state == actionrun.ActionRun.STATE_SCHEDULED

    @property
    def is_queued(self):
        return self.state == actionrun.ActionRun.STATE_QUEUED

    @property
    def is_running(self):
        return self.state == actionrun.ActionRun.STATE_RUNNING

    @property
    def is_starting(self):
        return self.state == actionrun.ActionRun.STATE_STARTING

    def __repr__(self):
        return str(self.__dict__)


class TestJobRunCollection(TestCase):
    def _mock_run(self, **kwargs):
        return MockJobRun(**kwargs)

    @setup
    def setup_runs(self):
        self.run_collection = jobrun.JobRunCollection(5, eventbus_publish=lambda: None)
        self.job_runs = [
            self._mock_run(state=actionrun.ActionRun.STATE_QUEUED, run_num=4),
            self._mock_run(state=actionrun.ActionRun.STATE_RUNNING, run_num=3),
        ] + [
            self._mock_run(
                state=actionrun.ActionRun.STATE_SUCCEEDED,
                run_num=i,
            ) for i in range(2, 0, -1)
        ]
        self.run_collection.runs.extend(self.job_runs)
        self.mock_node = mock.create_autospec(node.Node)

    def test__init__(self):
        assert_equal(self.run_collection.run_limit, 5)

    def test_from_config(self):
        job_config = mock.Mock(run_limit=20)
        runs = jobrun.JobRunCollection.from_config(job_config, eventbus_publish=lambda: None,)
        assert_equal(runs.run_limit, 20)

    def test_job_runs_from_state(self):
        state_data = [
            dict(
                run_num=i,
                job_name="thename",
                run_time="sometime",
                start_time="start_time",
                end_time="sometime",
                cleanup_run=None,
                runs=[],
            ) for i in range(3, -1, -1)
        ]
        action_graph = mock.create_autospec(actiongraph.ActionGraph)
        output_path = mock.create_autospec(filehandler.OutputPath)
        context = mock.Mock()
        node_pool = mock.create_autospec(node.NodePool)
        runs = jobrun.job_runs_from_state(
            state_data,
            action_graph,
            output_path,
            context,
            node_pool,
            eventbus_publish=lambda: None,
        )
        assert len(runs) == 4
        assert all([type(job) == jobrun.JobRun for job in runs])

    def test_build_new_run(self):
        autospec_method(self.run_collection.remove_old_runs)
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        mock_job = build_mock_job()
        job_run = self.run_collection.build_new_run(
            mock_job,
            run_time,
            self.mock_node,
        )
        assert_in(job_run, self.run_collection.runs)
        self.run_collection.remove_old_runs.assert_called_with()
        assert_equal(job_run.run_num, 5)
        assert_equal(job_run.job_name, mock_job.get_name.return_value)

    def test_build_new_run_manual(self):
        autospec_method(self.run_collection.remove_old_runs)
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        mock_job = build_mock_job()
        job_run = self.run_collection.build_new_run(
            mock_job,
            run_time,
            self.mock_node,
            True,
        )
        assert_in(job_run, self.run_collection.runs)
        self.run_collection.remove_old_runs.assert_called_with()
        assert_equal(job_run.run_num, 5)
        assert job_run.manual

    def test_cancel_pending(self):
        pending_runs = [mock.Mock() for _ in range(2)]
        autospec_method(
            self.run_collection.get_pending,
            return_value=pending_runs,
        )
        self.run_collection.cancel_pending()
        for pending_run in pending_runs:
            pending_run.cancel.assert_called_with()

    def test_cancel_pending_no_pending(self):
        autospec_method(self.run_collection.get_pending, return_value=[])
        self.run_collection.cancel_pending()

    def test_remove_pending(self):
        self.run_collection.remove_pending()
        assert_length(self.run_collection.runs, 3)
        assert_equal(self.run_collection.runs[0], self.job_runs[1])
        assert_call(self.job_runs[0].cleanup, 0)

    def test_get_run_by_state(self):
        state = actionrun.ActionRun.STATE_SUCCEEDED
        run = self.run_collection.get_run_by_state(state)
        assert_equal(run, self.job_runs[2])

    def test_get_run_by_state_no_match(self):
        state = actionrun.ActionRun.STATE_UNKNOWN
        run = self.run_collection.get_run_by_state(state)
        assert_equal(run, None)

    def test_get_run_by_num(self):
        run = self.run_collection.get_run_by_num(1)
        assert_equal(run.run_num, 1)

    def test_get_run_by_num_no_match(self):
        run = self.run_collection.get_run_by_num(7)
        assert_equal(run, None)

    def test_get_run_by_index(self):
        run = self.run_collection.get_run_by_index(-1)
        assert_equal(run, self.job_runs[0])
        run = self.run_collection.get_run_by_index(-2)
        assert_equal(run, self.job_runs[1])
        run = self.run_collection.get_run_by_index(0)
        assert_equal(run, self.job_runs[-1])
        run = self.run_collection.get_run_by_index(1)
        assert_equal(run, self.job_runs[-2])

    def test_get_run_by_index_invalid_index(self):
        run = self.run_collection.get_run_by_index(-5)
        assert_equal(run, None)
        run = self.run_collection.get_run_by_index(4)
        assert_equal(run, None)

    def test_get_run_by_state_short_name(self):
        run = self.run_collection.get_run_by_state_short_name("RUNN")
        assert_equal(run, self.job_runs[1])

    def test_get_run_by_state_short_name_no_match(self):
        run = self.run_collection.get_run_by_state_short_name("FAIL")
        assert_equal(run, None)

    def test_get_newest(self):
        run = self.run_collection.get_newest()
        assert_equal(run, self.job_runs[0])

    def test_get_newest_exclude_manual(self):
        run = self._mock_run(
            state=actionrun.ActionRun.STATE_RUNNING,
            run_num=5,
            manual=True,
        )
        self.job_runs.insert(0, run)
        newest_run = self.run_collection.get_newest(include_manual=False)
        assert_equal(newest_run, self.job_runs[1])

    def test_get_newest_no_runs(self):
        run_collection = jobrun.JobRunCollection(5, eventbus_publish=lambda: None,)
        assert_equal(run_collection.get_newest(), None)

    def test_pending(self):
        run_num = self.run_collection.next_run_num()
        scheduled_run = self._mock_run(
            run_num=run_num,
            state=actionrun.ActionRun.STATE_SCHEDULED,
        )
        self.run_collection.runs.appendleft(scheduled_run)
        pending = list(self.run_collection.get_pending())
        assert_length(pending, 2)
        assert_equal(pending, [scheduled_run, self.job_runs[0]])

    def test_get_active(self):
        starting_run = self._mock_run(
            run_num=self.run_collection.next_run_num(),
            state=actionrun.ActionRun.STATE_STARTING,
        )
        self.run_collection.runs.appendleft(starting_run)
        active = list(self.run_collection.get_active())
        assert_length(active, 2)
        assert_equal(active, [starting_run, self.job_runs[1]])

    def test_get_active_with_node(self):
        starting_run = self._mock_run(
            run_num=self.run_collection.next_run_num(),
            state=actionrun.ActionRun.STATE_STARTING,
        )
        starting_run.node = 'differentnode'
        self.run_collection.runs.appendleft(starting_run)
        active = list(self.run_collection.get_active('anode'))
        assert_length(active, 1)
        assert_equal(active, [self.job_runs[1]])

    def test_get_active_none(self):
        active = list(self.run_collection.get_active('bogus'))
        assert_length(active, 0)

    def test_get_first_queued(self):
        run_num = self.run_collection.next_run_num()
        second_queued = self._mock_run(
            run_num=run_num,
            state=actionrun.ActionRun.STATE_QUEUED,
        )
        self.run_collection.runs.appendleft(second_queued)

        first_queued = self.run_collection.get_first_queued()
        assert_equal(first_queued, self.job_runs[0])

    def test_get_first_queued_no_match(self):
        self.job_runs[0].state = actionrun.ActionRun.STATE_CANCELLED
        first_queued = self.run_collection.get_first_queued()
        assert not first_queued

    def test_get_next_to_finish(self):
        next_run = self.run_collection.get_next_to_finish()
        assert_equal(next_run, self.job_runs[1])

    def test_get_next_to_finish_by_node(self):
        self.job_runs[1].node = "seven"
        scheduled_run = self._mock_run(
            run_num=self.run_collection.next_run_num(),
            state=actionrun.ActionRun.STATE_SCHEDULED,
            node="nine",
        )
        self.run_collection.runs.appendleft(scheduled_run)

        next_run = self.run_collection.get_next_to_finish(node="seven")
        assert_equal(next_run, self.job_runs[1])

    def test_get_next_to_finish_none(self):
        next_run = self.run_collection.get_next_to_finish(node="seven")
        assert_equal(next_run, None)

        self.job_runs[1].state = None
        next_run = self.run_collection.get_next_to_finish()
        assert_equal(next_run, None)

    def test_get_next_run_num(self):
        assert_equal(self.run_collection.next_run_num(), 5)

    def test_get_next_run_num_first(self):
        run_collection = jobrun.JobRunCollection(5, eventbus_publish=lambda: None,)
        assert_equal(run_collection.next_run_num(), 0)

    def test_remove_old_runs(self):
        self.run_collection.run_limit = 1
        self.run_collection.remove_old_runs()

        assert_length(self.run_collection.runs, 1)
        assert_call(self.job_runs[-1].cleanup, 0)
        for job_run in self.run_collection.runs:
            assert_length(job_run.cancel.calls, 0)

    def test_remove_old_runs_none(self):
        self.run_collection.remove_old_runs()
        for job_run in self.job_runs:
            assert_length(job_run.cancel.calls, 0)

    def test_remove_old_runs_no_runs(self):
        run_collection = jobrun.JobRunCollection(4, eventbus_publish=lambda: None,)
        run_collection.remove_old_runs()

    def test_state_data(self):
        assert_length(self.run_collection.state_data, len(self.job_runs))

    def test_last_success(self):
        assert_equal(self.run_collection.last_success, self.job_runs[2])

    def test__str__(self):
        expected = "JobRunCollection[4(queued), 3(running), 2(succeeded), 1(succeeded)]"
        assert_equal(str(self.run_collection), expected)

    def test_get_action_runs(self):
        action_name = 'action_name'
        self.run_collection.runs = job_runs = [mock.Mock(), mock.Mock()]
        runs = self.run_collection.get_action_runs(action_name)
        expected = [
            job_run.get_action_run.return_value for job_run in job_runs
        ]
        assert_equal(runs, expected)
        for job_run in job_runs:
            job_run.get_action_run.assert_called_with(action_name)
