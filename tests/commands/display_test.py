from testify import TestCase, run, setup, assert_equal, turtle

from tron.commands.display import Color, DisplayServices, DisplayJobRuns
from tron.commands.display import DisplayActionRuns, DisplayJobs


class DisplayServicesTestCase(TestCase):

    @setup
    def setup_data(self):
        Color.enabled = True
        self.data = [
            dict(name="My Service",      state="stopped", live_count="4", enabled=True),
            dict(name="Another Service", state="running", live_count="2", enabled=False),
            dict(name="Yet another",     state="running", live_count="1", enabled=True)
        ]
        self.display = DisplayServices(80)

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
                id='something.23', state='FAIL', node='machine4',
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='2012-02-21 23:10:10',
                duration='2 days',
                manual=False,
            ),
            dict(
                id='something.55', state='QUE', node='machine3',
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
            node='machine4',
            command='echo 123',
            raw_command='echo 123',
            run_time='2012-01-20 23:11:23',
            start_time='2012-01-20 23:11:23',
            end_time='2012-02-21 23:10:10',
            duration='2 days',
            stdout=[],
            stderr=[]
        )

        Color.enabled = True
        self.options = turtle.Turtle(warn=False, num_displays=4)

    def test_format(self):
        display = DisplayJobRuns(options=self.options)
        out = display.format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 7)

    def test_format_with_warn(self):
        self.options.warn = True
        self.data = self.data[:1]
        self.data[0]['runs'] = [self.action_run]

        display = DisplayJobRuns(options=self.options)
        out = display.format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 16)
        assert lines[13].startswith('Actions:'), lines[13]


class DisplayJobsTestCase(TestCase):

    @setup
    def setup_data(self):
        Color.enabled = True
        self.options = turtle.Turtle(warn=False, num_displays=4)
        self.data = [
            dict(name='important_things', status='running',
                scheduler='DailyJob', last_success='unknown'),
            dict(name='other_thing', status='success',
                scheduler='DailyJob', last_success='2012-01-23 10:23:23',
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
                    node='machine4',
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
        display = DisplayJobs(self.options).format(self.data)
        out = display.format(self.data)
        lines = out.split('\n')
        return lines

    def test_format(self):
        lines = self.do_format()
        assert_equal(len(lines), 5)


class DisplayActionsTestCase(TestCase):

    @setup
    def setup_data(self):
        Color.enabled = True
        self.options = turtle.Turtle(
            warn=False,
            num_displays=6,
            stdout=False,
            stderr=False
        )
        self.data = {
            'id': 'something.23',
            'state': 'UNKWN',
            'node': 'something',
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
        display = DisplayActionRuns(options=self.options)
        out = display.format(self.data)
        return out.split('\n')

    def test_format(self):
        lines = self.format_lines()
        assert_equal(len(lines), 13)

    def test_format_warn(self):
        self.data['runs'] = [self.data['runs'][2]]
        self.data['runs'][0].update(self.details)
        self.options.warn = True
        lines = self.format_lines()
        assert_equal(len(lines), 11)


if __name__ == "__main__":
    run()
