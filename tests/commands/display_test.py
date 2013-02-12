from testify import TestCase, run, setup, assert_equal, turtle

from tron.commands.display import Color, DisplayServices, DisplayJobRuns
from tron.commands.display import DisplayActions, DisplayJobs


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
                duration='2 days'
            ),
            dict(
                id='something.55', state='QUE', node='machine3',
                run_time='2012-01-20 23:11:23',
                start_time='2012-01-20 23:11:23',
                end_time='',
                duration=''
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
        assert_equal(len(lines), 13)
        assert lines[9].startswith('Actions:'), lines[9]


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

    def test_format_with_warn(self):
        self.options.warn = True
        self.data = self.data[:1]
        self.data[0]['runs'] = self.run_data
        lines = self.do_format()
        assert_equal(len(lines), 26)
        assert lines[13] == lines[23] == 'Actions:'

    def test_format_job(self):
        self.options.display_preface = True
        job = self.data[1]
        job['runs'] = self.run_data
        display = DisplayJobs(self.options)
        out = display.format_job(job)
        lines = out.split("\n")
        assert_equal(len(lines), 18)
        assert lines[4:6] == job['action_names']

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
            'runs': [
                dict(
                    id='something.23.run_other_thing',
                    state='UNKWN',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='',
                    duration=''
                ),
                dict(
                    id='something.1.run_foo',
                    state='FAIL',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='2012-01-23 10:40:10.123456',
                    duration='1234.123456'
                ),
                dict(
                    id='something.23.run_other_thing',
                    state='QUE',
                    start_time='2012-01-23 10:10:10.123456',
                    end_time='',
                    duration=''
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
            'requirements':     ['.run_first_job']
        }

    def format_lines(self):
        display = DisplayActions(options=self.options)
        out = display.format(self.data)
        return out.split('\n')

    def format_action_run_lines(self):
        display = DisplayActions(options=self.options)
        out = display.format_action_run(self.details)
        return out.split('\n')

    def test_format(self):
        lines = self.format_lines()
        assert_equal(len(lines), 9)

    def test_format_warn(self):
        self.data['runs'] = [self.data['runs'][2]]
        self.data['runs'][0].update(self.details)
        self.options.warn = True
        lines = self.format_lines()
        assert_equal(len(lines), 11)

    def test_format_action_run(self):
        options = self.options
        options.stdout = options.stderr = options.display_preface = False
        lines = self.format_action_run_lines()
        assert_equal(len(lines), 15)

    def test_format_action_run_stdout(self):
        self.options.stdout = True
        lines = self.format_action_run_lines()
        assert_equal(lines, ['Stdout: '] + self.details['stdout'])

    def test_format_action_run_stderr(self):
        self.options.stderr = True
        lines = self.format_action_run_lines()
        assert_equal(lines, ['Stderr: '] + self.details['stderr'])

    def test_format_action_run_display_preface(self):
        self.options.display_preface = True
        lines = self.format_action_run_lines()
        assert_equal(len(lines), 19)
        assert lines[2] == 'Node: localhost'


if __name__ == "__main__":
    run()
