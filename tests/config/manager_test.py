import os
import shutil
import tempfile
import mock
from testify import TestCase, assert_equal, run, setup, teardown
from tron.config import manager


class ReadWriteTestCase(TestCase):

    def test_read_write(self):
        filename = tempfile.NamedTemporaryFile().name
        content = {'one': 'stars', 'two': 'beers'}
        manager.write(filename, content)
        actual = manager.read(filename)
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
        name = 'what'
        path = self.manager.build_file_path(name)
        assert_equal(path, os.path.join(self.temp_dir, name))

    def test_build_file_path_with_invalid_chars(self):
        path = self.manager.build_file_path('/etc/passwd')
        assert_equal(path, os.path.join(self.temp_dir, '_etc_passwd'))
        path = self.manager.build_file_path('../../etc/passwd')
        assert_equal(path, os.path.join(self.temp_dir, '______etc_passwd'))

    def test_read_config(self):
        name = 'name'
        path = os.path.join(self.temp_dir, name)
        manager.write(path, self.content)
        self.manifest.get_file_name.return_value = path
        config = self.manager.read_config(name)
        assert_equal(config, self.content)

    def test_write_config(self):
        name = 'filename'
        path = self.manager.build_file_path(name)
        self.manifest.get_file_name.return_value = path
        self.manager.write_config(name, self.content)
        assert_equal(manager.read(path), self.content)
        self.manifest.get_file_name.assert_called_with(name)
        assert not self.manifest.add.call_count

    def test_write_config_new_name(self):
        name = 'filename2'
        path = self.manager.build_file_path(name)
        self.manifest.get_file_name.return_value = None
        self.manager.write_config(name, self.content)
        assert_equal(manager.read(path), self.content)
        self.manifest.get_file_name.assert_called_with(name)
        self.manifest.add.assert_called_with(name, path)


if __name__ == "__main__":
    run()