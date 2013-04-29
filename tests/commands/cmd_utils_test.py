import contextlib
import mock
from testify import TestCase, assert_equal, setup_teardown
import tron
from tron.commands import cmd_utils


class GetConfigTestCase(TestCase):

    @setup_teardown
    def patch_environment(self):
        with contextlib.nested(
            mock.patch('tron.commands.cmd_utils.opener', autospec=True),
            mock.patch('tron.commands.cmd_utils.yaml', autospec=True),
        ) as (self.mock_opener, self.mock_yaml):
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


class BuildOptionParserTestCase(TestCase):

    def test_build_option_parser(self):
        """Assert that we don't set default options so that we can load
        the defaults from the config.
        """
        parser_class = mock.Mock()
        usage = 'Something'
        parser = cmd_utils.build_option_parser(usage, parser_class=parser_class)
        assert_equal(parser, parser_class.return_value)
        parser_class.assert_called_with(
            usage, version="%%prog %s" % tron.__version__)
        assert_equal(parser.add_option.call_count, 3)

        options = [call[1] for call in parser.add_option.mock_calls]
        expected = [('-v', '--verbose'), ('--server',), ('-s', '--save')]
        assert_equal(options, expected)

        defaults = [
            call[2].get('default') for call in parser.add_option.mock_calls]
        assert_equal(defaults, [None, None, None])
