from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import mock

from testifycompat import assert_equal
from testifycompat import assert_raises
from testifycompat import run
from testifycompat import setup
from testifycompat import TestCase
from tron import command_context
from tron import node
from tron import scheduler
from tron.core import actionrun
from tron.core import job
from tron.core import jobrun
from tron.core.jobrun import JobRunCollection


class TestEmptyContext(TestCase):
    @setup
    def build_context(self):
        self.context = command_context.CommandContext(None)

    def test__getitem__(self):
        assert_raises(KeyError, self.context.__getitem__, 'foo')

    def test_get(self):
        assert not self.context.get('foo')


class TestBuildFilledContext(TestCase):
    def test_build_filled_context_no_objects(self):
        output = command_context.build_filled_context()
        assert not output.base
        assert not output.next

    def test_build_filled_context_single(self):
        output = command_context.build_filled_context(
            command_context.JobContext,
        )
        assert isinstance(output.base, command_context.JobContext)
        assert not output.next

    def test_build_filled_context_chain(self):
        objs = [command_context.JobContext, command_context.JobRunContext]
        output = command_context.build_filled_context(*objs)
        assert isinstance(output.base, objs[1])
        assert isinstance(output.next.base, objs[0])
        assert not output.next.next


class SimpleContextTestCaseBase(TestCase):
    __test__ = False

    def test_hit(self):
        assert_equal(self.context['foo'], 'bar')

    def test_miss(self):
        assert_raises(KeyError, self.context.__getitem__, 'your_mom')

    def test_get_hit(self):
        assert_equal(self.context.get('foo'), 'bar')

    def test_get_miss(self):
        assert not self.context.get('unknown')


class SimpleDictContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.context = command_context.CommandContext(dict(foo='bar'))


class SimpleObjectContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        class Obj(object):
            foo = 'bar'

        self.context = command_context.CommandContext(Obj)


class ChainedDictContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.next_context = command_context.CommandContext(
            dict(foo='bar', next_foo='next_bar'),
        )
        self.context = command_context.CommandContext(
            dict(),
            self.next_context,
        )

    def test_chain_get(self):
        assert_equal(self.context['next_foo'], 'next_bar')


class ChainedDictOverrideContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        self.next_context = command_context.CommandContext(
            dict(foo='your mom', next_foo='next_bar'),
        )
        self.context = command_context.CommandContext(
            dict(foo='bar'),
            self.next_context,
        )

    def test_chain_get(self):
        assert_equal(self.context['next_foo'], 'next_bar')


class ChainedObjectOverrideContextTestCase(SimpleContextTestCaseBase):
    @setup
    def build_context(self):
        class MyObject(TestCase):
            pass

        obj = MyObject()
        obj.foo = 'bar'

        self.next_context = command_context.CommandContext(
            dict(foo='your mom', next_foo='next_bar'),
        )
        self.context = command_context.CommandContext(obj, self.next_context)

    def test_chain_get(self):
        assert_equal(self.context['next_foo'], 'next_bar')


class TestJobContext(TestCase):
    @setup
    def setup_job(self):
        self.last_success = mock.Mock(run_time=datetime.datetime(2012, 3, 14))
        mock_scheduler = mock.create_autospec(scheduler.ConstantScheduler)
        run_collection = mock.create_autospec(
            JobRunCollection,
            last_success=self.last_success,
        )
        self.job = job.Job(
            "jobname",
            mock_scheduler,
            run_collection=run_collection,
            eventbus_publish=lambda: None,
        )
        self.context = command_context.JobContext(self.job)

    def test_name(self):
        assert_equal(self.context.name, self.job.name)

    def test__getitem__last_success(self):
        item = self.context["last_success#day-1"]
        expected_date = self.last_success.run_time - datetime.timedelta(days=1)
        assert_equal(item, str(expected_date.day))

        item = self.context["last_success#shortdate"]
        assert_equal(item, "2012-03-14")

    def test__getitem__last_success_bad_date_spec(self):
        name = "last_success#beers-3"
        assert_raises(KeyError, lambda: self.context[name])

    def test__getitem__last_success_bad_date_name(self):
        name = "first_success#shortdate-1"
        assert_raises(KeyError, lambda: self.context[name])

    def test__getitem__last_success_no_date_spec(self):
        name = "last_success"
        assert_raises(KeyError, lambda: self.context[name])

    def test__getitem__missing(self):
        assert_raises(KeyError, lambda: self.context['bogus'])


class TestJobRunContext(TestCase):
    @setup
    def setup_context(self):
        self.jobrun = mock.create_autospec(jobrun.JobRun, run_time='sometime')
        self.context = command_context.JobRunContext(self.jobrun)

    def test_cleanup_job_status(self):
        self.jobrun.action_runs.is_failed = False
        self.jobrun.action_runs.is_complete_without_cleanup = True
        assert_equal(self.context.cleanup_job_status, 'SUCCESS')

    def test_cleanup_job_status_failure(self):
        self.jobrun.action_runs.is_failed = True
        assert_equal(self.context.cleanup_job_status, 'FAILURE')

    def test_runid(self):
        assert_equal(self.context.runid, self.jobrun.id)

    @mock.patch('tron.command_context.timeutils.DateArithmetic', autospec=True)
    def test__getitem__(self, mock_date_math):
        name = 'date_name'
        time_value = self.context[name]
        mock_date_math.parse.assert_called_with(name, self.jobrun.run_time)
        assert_equal(time_value, mock_date_math.parse.return_value)


class TestActionRunContext(TestCase):
    @setup
    def build_context(self):
        mock_node = mock.create_autospec(node.Node, hostname='something')
        self.action_run = mock.create_autospec(
            actionrun.ActionRun,
            action_name='something',
            node=mock_node,
        )
        self.context = command_context.ActionRunContext(self.action_run)

    def test_actionname(self):
        assert_equal(self.context.actionname, self.action_run.action_name)

    def test_node_hostname(self):
        assert_equal(self.context.node, self.action_run.node.hostname)


class TestFiller(TestCase):
    @setup
    def setup_filler(self):
        self.filler = command_context.Filler()

    def test_filler_with_job__getitem__(self):
        context = command_context.JobContext(self.filler)
        todays_date = datetime.date.today().strftime("%Y-%m-%d")
        assert_equal(context['last_success#shortdate'], todays_date)

    def test_filler_with_job_run__getitem__(self):
        context = command_context.JobRunContext(self.filler)
        todays_date = datetime.date.today().strftime("%Y-%m-%d")
        assert_equal(context['shortdate'], todays_date)


if __name__ == '__main__':
    run()
