import os
import shutil
import tempfile
import mock
from testify import TestCase, assert_equal, run, setup, teardown
import yaml
from tests.assertions import assert_raises
from tests.testingutils import autospec_method
from tron.config import manager, ConfigError


class FromStringTestCase(TestCase):

    def test_from_string_valid(self):
        content = "{'one': 'thing', 'another': 'thing'}\n"
        actual = manager.from_string(content)
        expected = {'one': 'thing', 'another': 'thing'}
        assert_equal(actual, expected)

    def test_from_string_invalid(self):
        content = "{} asdf"
        assert_raises(ConfigError, manager.from_string, content)

class ReadWriteTestCase(TestCase):

    @setup
    def setup_tempfile(self):
        self.filename = tempfile.NamedTemporaryFile().name

    @teardown
    def teardown_tempfile(self):
        os.unlink(self.filename)

    def test_read_write(self):
        content = {'one': 'stars', 'two': 'beers'}
        manager.write(self.filename, content)
        actual = manager.read(self.filename)
        assert_equal(content, actual)

    def test_read_raw_write_raw(self):
        content = "Some string"
        manager.write_raw(self.filename, content)
        actual = manager.read_raw(self.filename)
        assert_equal(content, actual)

class ManifestFileTestCase(TestCase):

    @setup
    def setup_manifest(self):
        self.temp_dir = tempfile.mkdtemp()
        self.manifest = manager.ManifestFile(self.temp_dir)
        self.manifest.create()

    @teardown
    def teardown_dir(self):
        shutil.rmtree(self.temp_dir)

    @mock.patch('tron.config.manager.os.path')
    @mock.patch('tron.config.manager.write', autospec=True)
    def test_create_exists(self, mock_write, mock_os):
        mock_os.isfile.return_value = True
        self.manifest.create()
        assert not mock_write.call_count

    def test_create(self):
        assert_equal(manager.read(self.manifest.filename), {})

    def test_add(self):
        self.manifest.add('zing', 'zing.yaml')
        expected = {'zing': 'zing.yaml'}
        assert_equal(manager.read(self.manifest.filename), expected)

    def test_get_file_mapping(self):
        file_mapping = {
            'one': 'a.yaml',
            'two': 'b.yaml',
        }
        manager.write(self.manifest.filename, file_mapping)
        assert_equal(self.manifest.get_file_mapping(), file_mapping)


class ConfigManagerTestCase(TestCase):

    content = {'one': 'stars', 'two': 'other'}
    raw_content = "{'one': 'stars', 'two': 'other'}\n"

    @setup
    def setup_config_manager(self):
        self.temp_dir = tempfile.mkdtemp()
        self.manager = manager.ConfigManager(self.temp_dir)
        self.manifest = mock.create_autospec(manager.ManifestFile)
        self.manager.manifest = self.manifest

    @teardown
    def teardown_dir(self):
        shutil.rmtree(self.temp_dir)

    def test_build_file_path(self):
        path = self.manager.build_file_path('what')
        assert_equal(path, os.path.join(self.temp_dir, 'what.yaml'))

    def test_build_file_path_with_invalid_chars(self):
        path = self.manager.build_file_path('/etc/passwd')
        assert_equal(path, os.path.join(self.temp_dir, '_etc_passwd.yaml'))
        path = self.manager.build_file_path('../../etc/passwd')
        assert_equal(path, os.path.join(self.temp_dir, '______etc_passwd.yaml'))

    def test_read_raw_config(self):
        name = 'name'
        path = os.path.join(self.temp_dir, name)
        manager.write(path, self.content)
        self.manifest.get_file_name.return_value = path
        config = self.manager.read_raw_config(name)
        assert_equal(config, yaml.dump(self.content))

    def test_write_config(self):
        name = 'filename'
        path = self.manager.build_file_path(name)
        self.manifest.get_file_name.return_value = path
        autospec_method(self.manager.validate_fragment)
        self.manager.write_config(name, self.raw_content)
        assert_equal(manager.read(path), self.content)
        self.manifest.get_file_name.assert_called_with(name)
        assert not self.manifest.add.call_count
        self.manager.validate_fragment.assert_called_with(name, self.content)

    def test_write_config_new_name(self):
        name = 'filename2'
        path = self.manager.build_file_path(name)
        self.manifest.get_file_name.return_value = None
        autospec_method(self.manager.validate_fragment)
        self.manager.write_config(name, self.raw_content)
        assert_equal(manager.read(path), self.content)
        self.manifest.get_file_name.assert_called_with(name)
        self.manifest.add.assert_called_with(name, path)

    def test_validate_fragment(self):
        autospec_method(self.manager.load)
        name = 'the_name'
        self.manager.validate_fragment(name, self.content)
        container = self.manager.load.return_value
        container.add.assert_called_with(name, self.content)
        container.validate.assert_called_with()

    @mock.patch('tron.config.manager.read')
    @mock.patch('tron.config.manager.config_parse.ConfigContainer', autospec=True)
    def test_load(self, mock_config_container, mock_read):
        content_items = self.content.items()
        self.manifest.get_file_mapping().iteritems.return_value = content_items
        container = self.manager.load()
        self.manifest.get_file_mapping.assert_called_with()
        assert_equal(container, mock_config_container.create.return_value)

        expected = dict((name, call.return_value)
            for ((name, _), call) in zip(content_items, mock_read.mock_calls))
        mock_config_container.create.assert_called_with(expected)

    def test_get_hash_default(self):
        self.manifest.__contains__.return_value = False
        hash_digest = self.manager.get_hash('name')
        assert_equal(hash_digest, self.manager.DEFAULT_HASH)

    def test_get_hash(self):
        content = "OkOkOk"
        autospec_method(self.manager.read_raw_config, return_value=content)
        self.manifest.__contains__.return_value = True
        hash_digest = self.manager.get_hash('name')
        assert_equal(hash_digest, manager.hash_digest(content))


if __name__ == "__main__":
    run()