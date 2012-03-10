from testify import TestCase, run, setup, assert_equal, turtle

from tron.commands.display import Color, DisplayServices, DisplayJobRuns
from tron.commands.display import DisplayActions, DisplayJobs


class DisplayServicesTestCase(TestCase):

    @setup
    def setup_data(self):
        Color.enabled = True
        self.data = [
            dict(name="My Service", status="stopped", count="4"),
            dict(name="Another Service", status="running", count="2"),
            dict(name="Yet another", status="running", count="1")
        ]

    def test_format(self):
        display = DisplayServices(80)
        out = display.format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 6)
        assert lines[3].startswith('Another')

    def test_format_no_data(self):
        display = DisplayServices(80)
        out = display.format([])
        lines = out.split("\n")
        assert_equal(len(lines), 3)


class DisplayJobRunsTestCase(TestCase):

    run_data = [
        dict(
            id='something.23.more', state='FAIL', node='machine4', 
            run_time='2012-01-20 23:11:23',
            start_time='2012-01-20 23:11:23',
            end_time='2012-02-21 23:10:10',
            duration='2 days'
        ),
        dict(
            id='something.55.other', state='QUE', node='machine3',
            run_time='2012-01-20 23:11:23',
            start_time='2012-01-20 23:11:23',
            end_time='',
            duration=''
        )
    ]

    @setup
    def setup_data(self):
        Color.enabled = True
        self.options = turtle.Turtle(warn=False, num_displays=4)
        self.data = self.run_data

    def test_format(self):
        display = DisplayJobRuns(80, options=self.options)
        out = display.format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 7)

    # TODO: test for warn=True
    def test_format_with_warn(self):
        pass


class DisplayJobsTestCase(TestCase):

    @setup
    def setup_data(self):
        Color.enabled = True
        self.options = turtle.Turtle(warn=False, num_displays=4)
        self.data = [
            dict(name='important_things', status='running', 
                scheduler='DailyJob', last_success='unknown'),
            dict(name='other_thing', status='success', 
                scheduler='DailyJob', last_success='2012-01-23 10:23:23'),
        ]
        self.details = {
            'name': '.foo',
            'scheduler': 'DailyJob',
            'action_names': ['one', 'two', 'three'],
            'node_pool': ['machine1', 'machine2'],
            'runs': DisplayJobRunsTestCase.run_data
        }

    def do_format(self):
        display = DisplayJobs(80, self.options).format(self.data)
        out = display.format(self.data)
        lines = out.split('\n')
        return lines

    def test_format(self):
        lines = self.do_format()
        assert_equal(len(lines), 5)

    # TODO: add action_run details
    def test_format_with_warn(self):
        return
        self.options.warn = True
        for data in self.data:
            data['details'] = self.details
        lines = self.do_format()
        assert_equal(len(lines), 19)

    def test_format_job(self):
        pass

class DisplayActionsTestCase(TestCase):

    @setup
    def setup_data(self):
        Color.enabled = True
        self.options = turtle.Turtle(warn=False, num_displays=6)
        self.data = [
            dict(
                id='something.23.run_other_thing', state='UNKWN',
                start_time='2012-01-23 10:10:10.123456',
                end_time='',
                duration=''
            ),
            dict(
                id='something.1.run_foo', state='FAIL',
                start_time='2012-01-23 10:10:10.123456',
                end_time='2012-01-23 10:40:10.123456',
                duration='1234.123456'
            ),
            dict(
                id='something.23.run_other_thing', state='QUE',
                start_time='2012-01-23 10:10:10.123456',
                end_time='',
                duration=''
            ),
        ]
        self.details = {
            'stdout': ['Blah', 'blah', 'blah'],
            'stderr': ['Crash', 'and', 'burn'],
            'command': '/bin/bash ./runme.sh now',
            'raw_command': 'bash runme.sh now',
            'requirements': ['.run_first_job']
        }

    def test_format(self):
        display = DisplayActions(80, options=self.options)
        out = display.format(self.data)
        lines = out.split('\n')
        assert_equal(len(lines), 6)

    def test_format_warn(self):
        data = self.data[2]
        data['details'] = self.details
        self.options.warn = True
        display = DisplayActions(80, options=self.options)
        out = display.format([data])
        lines = out.split('\n')
        assert_equal(len(lines), 8)

    def test_format_action_run(self):
        options = self.options
        options.stdout = options.stderr = options.display_preface = False
        display = DisplayActions(80, options=self.options)
        out = display.format_action_run(self.details)
        lines = out.split('\n')
        assert_equal(len(lines), 14)
        

    # TODO: test format_action_run with other options


if __name__ == "__main__":
    run()
