from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from testifycompat import setup
from testifycompat import setup_teardown
from testifycompat import TestCase
from tron.commands import display
from tron.commands.display import DisplayActionRuns
from tron.commands.display import DisplayJobRuns
from tron.commands.display import DisplayJobs
from tron.core import actionrun
from tron.core import job


class TestDisplayJobRuns(TestCase):
    @setup
    def setup_data(self):
        self.data = [
            dict(
                id='something.23',
                state='FAIL',
                node=mock.MagicMock(),
                run_num=23,
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='2012-02-21 23:10:10',
                duration='2 days',
                manual=False,
            ),
            dict(
                id='something.55',
                state='QUE',
                node=mock.MagicMock(),
                run_num=55,
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='',
                duration='',
                manual=False,
            ),
        ]

    def test_format(self):
        out = DisplayJobRuns().format(self.data)
        lines = out.split('\n')
        assert len(lines) == 7


class TestDisplayJobs(TestCase):
    @setup
    def setup_data(self):
        self.data = [
            dict(
                name='important_things',
                status='running',
                scheduler=mock.MagicMock(),
                last_success=None,
            ),
            dict(
                name='other_thing',
                status='enabled',
                scheduler=mock.MagicMock(),
                last_success='2012-01-23 10:23:23',
            ),
        ]

    def do_format(self):
        out = DisplayJobs().format(self.data)
        lines = out.split('\n')
        return lines

    def test_format(self):
        lines = self.do_format()
        assert len(lines) == 5


class TestDisplayActions(TestCase):
    @setup
    def setup_data(self):
        self.data = {
            'id':
                'something.23',
            'state':
                'UNKWN',
            'node': {
                'hostname': 'something',
                'username': 'a',
            },
            'run_time':
                'sometime',
            'start_time':
                'sometime',
            'end_time':
                'sometime',
            'manual':
                False,
            'runs': [
                dict(
                    id='something.23.run_other_thing',
                    state='unknown',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='',
                    duration='',
                    run_time='sometime',
                ),
                dict(
                    id='something.1.run_foo',
                    state='failed',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='2012-01-23 10:40:10.123456',
                    duration='1234.123456',
                    run_time='sometime',
                ),
                dict(
                    id='something.23.run_other_thing',
                    state='queued',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='',
                    duration='',
                    run_time='sometime',
                ),
                dict(
                    id='something.42.weird_run',
                    state='unknown',
                    start_time=None,
                    end_time=None,
                    duration="",
                    run_time=None,
                ),
                dict(
                    id='something.43.skipped',
                    state='skipped',
                    start_time="2019-07-15 18:12:05",
                    end_time="2019-07-16 01:31:50",
                    duration="7:19:44.506211",
                ),
                dict(
                    id='something.43.running',
                    state='running',
                    start_time="2019-07-15 18:12:05",
                    end_time=None,
                    duration="7:19:44.506211",
                ),
            ],
        }
        self.details = {
            'id': 'something.1.foo',
            'state': 'FAIL',
            'node': 'localhost',
            'stdout': ['Blah', 'blah', 'blah'],
            'stderr': ['Crash', 'and', 'burn'],
            'command': '/bin/bash ./runme.sh now',
            'raw_command': 'bash runme.sh now',
            'requirements': ['.run_first_job'],
        }

    def format_lines(self):
        out = DisplayActionRuns().format(self.data)
        return out.split('\n')

    def test_format(self):
        lines = self.format_lines()
        assert len(lines) == 16, "\n".join(lines)


class TestAddColorForState(TestCase):
    @setup_teardown
    def enable_color(self):
        with display.Color.enable():
            yield

    def test_add_red(self):
        text = display.add_color_for_state(actionrun.ActionRun.FAILED, )
        assert text.startswith(display.Color.colors['red']), text

    def test_add_green(self):
        text = display.add_color_for_state(actionrun.ActionRun.RUNNING, )
        assert text.startswith(display.Color.colors['green']), text

    def test_add_blue(self):
        text = display.add_color_for_state(job.Job.STATUS_DISABLED)
        assert text.startswith(display.Color.colors['blue']), text


class TestDisplayNode(TestCase):

    node_source = {
        'name': 'name',
        'hostname': 'hostname',
        'username': 'username',
    }

    def test_display_node(self):
        result = display.display_node(self.node_source)
        assert result == 'username@hostname'

    def test_display_node_pool(self):
        source = {'name': 'name', 'nodes': [self.node_source]}
        result = display.display_node_pool(source)
        assert result == 'name (1 node(s))'
