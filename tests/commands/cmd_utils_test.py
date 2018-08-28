from __future__ import absolute_import
from __future__ import unicode_literals

import argparse

import mock

from testifycompat import assert_equal
from testifycompat import assert_in
from testifycompat import setup_teardown
from testifycompat import TestCase
from tron.commands import cmd_utils


class TestGetConfig(TestCase):
    @setup_teardown
    def patch_environment(self):
        with mock.patch('tron.commands.cmd_utils.opener', autospec=True) as self.mock_opener, \
                mock.patch('tron.commands.cmd_utils.yaml', autospec=True) as self.mock_yaml:
            yield

    def test_read_config_missing(self):
        self.mock_opener.side_effect = IOError
        assert_equal(cmd_utils.read_config(), {})

    def test_read_config(self):
        assert_equal(cmd_utils.read_config(), self.mock_yaml.load.return_value)

    @mock.patch('tron.commands.cmd_utils.os.access', autospec=True)
    def test_get_client_config(self, mock_access):
        mock_access.return_value = False
        config = cmd_utils.get_client_config()
        assert_equal(mock_access.call_count, 2)
        assert_equal(config, {})

    def test_filter_jobs_actions_runs_with_nothing(self):
        inputs = [
            "M.foo",
            "M.foo.1",
            "M.foo.1.action1",
            "M.foo.2.action1",
            "M.bar",
            "M.bar.1.action",
        ]
        prefix = ""
        expected = ["M.foo", "M.bar"]
        assert_equal(
            cmd_utils.filter_jobs_actions_runs(
                prefix,
                inputs,
            ),
            expected,
        )

    def test_filter_jobs_actions_runs_with_almost_a_job(self):
        inputs = [
            "M.foo",
            "M.foo.1",
            "M.foo.1.action1",
            "M.foo.2.action1",
            "M.bar.1.action",
        ]
        prefix = "M.f"
        expected = ["M.foo"]
        assert_equal(
            cmd_utils.filter_jobs_actions_runs(
                prefix,
                inputs,
            ),
            expected,
        )

    def test_filter_jobs_actions_runs_with_a_job_run(self):
        inputs = [
            "M.foo",
            "M.foo.1",
            "M.foo.1.action1",
            "M.foo.2",
            "M.foo.2.action1",
            "M.bar.1.action",
        ]
        prefix = "M.foo."
        expected = ["M.foo.1", "M.foo.2"]
        assert_equal(
            cmd_utils.filter_jobs_actions_runs(
                prefix,
                inputs,
            ),
            expected,
        )

    def test_filter_jobs_actions_runs_with_a_job_run_and_id(self):
        inputs = [
            "M.foo",
            "M.foo.1",
            "M.foo.1.action1",
            "M.foo.2.action1",
            "M.bar.1.action",
        ]
        prefix = "M.foo.1"
        expected = ["M.foo.1", "M.foo.1.action1"]
        assert_equal(
            cmd_utils.filter_jobs_actions_runs(
                prefix,
                inputs,
            ),
            expected,
        )


class TestBuildOptionParser(TestCase):
    def test_build_option_parser(self):
        """Assert that we don't set default options so that we can load
        the defaults from the config.
        """
        usage = 'Something'
        epilog = 'Something'
        argparse.ArgumentParser = mock.Mock()
        parser = cmd_utils.build_option_parser(
            usage=usage,
            epilog=epilog,
        )
        argparse.ArgumentParser.assert_called_with(
            usage=usage,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=epilog,
        )
        assert_equal(parser.add_argument.call_count, 4)

        args = [call[1] for call in parser.add_argument.mock_calls]
        expected = [
            ('--version', ),
            ('-v', '--verbose'),
            ('--server', ),
            ('-s', '--save'),
        ]
        assert_equal(args, expected)

        defaults = [
            call[2].get('default') for call in parser.add_argument.mock_calls
        ]
        assert_equal(defaults, [None, None, None, None])


class TestSuggestions(TestCase):
    def test_suggest_possibilities_none(self):
        expected = ""
        actual = cmd_utils.suggest_possibilities(word='FOO', possibilities=[])
        assert_equal(actual, expected)

    def test_suggest_possibilities_many(self):
        expected = "FOOO, FOOBAR"
        actual = cmd_utils.suggest_possibilities(
            word='FOO', possibilities=["FOOO", "FOOBAR"]
        )
        assert_in(expected, actual)

    def test_suggest_possibilities_one(self):
        expected = "FOOBAR?"
        actual = cmd_utils.suggest_possibilities(
            word='FOO', possibilities=["FOOBAR", "BAZ"]
        )
        assert_in(expected, actual)
