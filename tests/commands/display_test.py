import mock
from testify import TestCase, run, setup, assert_equal
from testify import setup_teardown
from tron.commands import display

from tron.commands.display import DisplayServices, DisplayJobRuns
from tron.commands.display import DisplayActionRuns, DisplayJobs
from tron.core import actionrun, service


class DisplayServicesTestCase(TestCase):

    @setup
    def setup_data(self):
        self.data = [
            dict(name="My Service",      state="stopped", live_count="4", enabled=True),
            dict(name="Another Service", state="running", live_count="2", enabled=False),
            dict(name="Yet another",     state="running", live_count="1", enabled=True)
        ]
        self.display = DisplayServices()

    def test_format(self):
        out = self.display.format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 6)
        assert lines[3].startswith('Another')

    def test_format_no_data(self):
        out = self.display.format([])
        lines = out.split("\n")
        assert_equal(len(lines), 4)
        assert_equal(lines[2], 'No Services')


class DisplayJobRunsTestCase(TestCase):

    @setup
    def setup_data(self):
        self.data = [
            dict(
                id='something.23', state='FAIL', node=mock.MagicMock(),
                run_num=23,
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='2012-02-21 23:10:10',
                duration='2 days',
                manual=False,
            ),
            dict(
                id='something.55', state='QUE', node=mock.MagicMock(),
                run_num=55,
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='',
                duration='',
                manual=False,
            )
        ]

        self.action_run = dict(
            id='something.23.other',
            name='other',
            state='FAIL',
            node=mock.MagicMock(),
            command='echo 123',
            raw_command='echo 123',
            run_time='2012-01-20 23:11:23',
            start_time='2012-01-20 23:11:23',
            end_time='2012-02-21 23:10:10',
            duration='2 days',
            stdout=[],
            stderr=[]
        )


    def test_format(self):
        out = DisplayJobRuns().format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 7)


class DisplayJobsTestCase(TestCase):

    @setup
    def setup_data(self):
        self.data = [
            dict(name='important_things', status='running',
                scheduler=mock.MagicMock(), last_success='unknown'),
            dict(name='other_thing', status='success',
                scheduler=mock.MagicMock(), last_success='2012-01-23 10:23:23',
                action_names=['other', 'first'],
                node_pool=['blam']),
        ]
        self.run_data = [
            dict(
                id='something.23', state='FAIL', node='machine4',
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='2012-02-21 23:10:10',
                duration='2 days',
                runs=[dict(
                    id='something.23.other',
                    name='other',
                    state='FAIL',
                    node=mock.MagicMock(),
                    command='echo 123',
                    raw_command='echo 123',
                    run_time='2012-01-20 23:11:23',
                    start_time='2012-01-20 23:11:23',
                    end_time='2012-02-21 23:10:10',
                    duration='2 days',
                    stdout=[],
                    stderr=[]
                )]
            ),
            dict(
                id='something.55', state='QUE', node='machine3',
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='',
                duration='',
                runs=[]
            )
        ]

    def do_format(self):
        out = DisplayJobs().format(self.data)
        lines = out.split('\n')
        return lines

    def test_format(self):
        lines = self.do_format()
        assert_equal(len(lines), 5)


class DisplayActionsTestCase(TestCase):

    @setup
    def setup_data(self):
        self.data = {
            'id': 'something.23',
            'state': 'UNKWN',
            'node': {'hostname': 'something', 'username': 'a'},
            'run_time': 'sometime',
            'start_time': 'sometime',
            'end_time': 'sometime',
            'manual': False,
            'runs': [
                dict(
                    id='something.23.run_other_thing',
                    state='UNKWN',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='',
                    duration='',
                    run_time='sometime',
                ),
                dict(
                    id='something.1.run_foo',
                    state='FAIL',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='2012-01-23 10:40:10.123456',
                    duration='1234.123456',
                    run_time='sometime',
                ),
                dict(
                    id='something.23.run_other_thing',
                    state='QUE',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='',
                    duration='',
                    run_time='sometime',
                ),
            ]
        }
        self.details = {
            'id':               'something.1.foo',
            'state':            'FAIL',
            'node':             'localhost',
            'stdout':           ['Blah', 'blah', 'blah'],
            'stderr':           ['Crash', 'and', 'burn'],
            'command':          '/bin/bash ./runme.sh now',
            'raw_command':      'bash runme.sh now',
            'requirements':     ['.run_first_job'],
        }

    def format_lines(self):
        out = DisplayActionRuns().format(self.data)
        return out.split('\n')

    def test_format(self):
        lines = self.format_lines()
        assert_equal(len(lines), 13)


class AddColorForStateTestCase(TestCase):

    @setup_teardown
    def enable_color(self):
        with display.Color.enable():
            yield

    def test_add_red(self):
        text = display.add_color_for_state(actionrun.ActionRun.STATE_FAILED.name)
        assert text.startswith(display.Color.colors['red']), text

    def test_add_green(self):
        text = display.add_color_for_state(actionrun.ActionRun.STATE_RUNNING.name)
        assert text.startswith(display.Color.colors['green']), text

    def test_add_blue(self):
        text = display.add_color_for_state(service.ServiceState.DISABLED)
        assert text.startswith(display.Color.colors['blue']), text


class DisplayNodeTestCase(TestCase):

    node_source = {
        'name': 'name',
        'hostname': 'hostname',
        'username': 'username'}

    def test_display_node(self):
        result = display.display_node(self.node_source)
        assert_equal(result, 'username@hostname')

    def test_display_node_pool(self):
        source = {'name': 'name', 'nodes': [self.node_source]}
        result = display.display_node_pool(source)
        assert_equal(result, 'name (1 node(s))')

class DisplaySchedulerTestCase(TestCase):

    def test_display_scheduler_no_jitter(self):
        source = {'value': '5 minutes', 'type': 'interval', 'jitter': ''}
        result = display.display_scheduler(source)
        assert_equal(result, 'interval 5 minutes')

    def test_display_scheduler_with_jitter(self):
        source = {
            'value': '5 minutes',
            'type': 'interval',
            'jitter': ' (+/- 2 min)'}
        result = display.display_scheduler(source)
        assert_equal(result, 'interval 5 minutes%s' % (source['jitter']))



if __name__ == "__main__":
    run()
