import os
import tempfile

from testify import TestCase, run, setup, assert_equal, teardown
import yaml
from tron.serialize.runstate import yamlstore

class YamlStateStoreTestCase(TestCase):

    @setup
    def setup_store(self):
        self.filename = os.path.join(tempfile.gettempdir(), 'yaml_state')
        self.store = yamlstore.YamlStateStore(self.filename)
        self.test_data = {
            'one': {'a': 1},
            'two': {'b': 2},
            'three': {'c': 3}
        }

    @teardown
    def teardown_store(self):
        try:
            os.unlink(self.filename)
        except OSError:
            pass

    def test_restore(self):
        with open(self.filename, 'w') as fh:
            yaml.dump(self.test_data, fh)

        keys = [yamlstore.YamlKey('one', 'a'), yamlstore.YamlKey('three', 'c')]
        state_data = self.store.restore(keys)
        assert_equal(self.store.buffer, self.test_data)

        expected = {keys[0]: 1, keys[1]: 3}
        assert_equal(expected, state_data)

    def test_restore_missing_type_key(self):
        with open(self.filename, 'w') as fh:
            yaml.dump(self.test_data, fh)

        keys = [yamlstore.YamlKey('seven', 'a')]
        state_data = self.store.restore(keys)
        assert_equal(self.store.buffer, self.test_data)
        assert_equal({}, state_data)

    def test_restore_file_missing(self):
        state_data = self.store.restore(['some', 'keys'])
        assert_equal(state_data, {})

    def test_save(self):
        expected = {'one': {'five': 'dataz'}, 'two': {'seven': 'stars'}}

        key_value_pairs = [
            (yamlstore.YamlKey('one', 'five'), 'barz')
        ]
        # Save first
        self.store.save(key_value_pairs)

        # Save second
        key_value_pairs = [
            (yamlstore.YamlKey('two', 'seven'), 'stars'),
            (yamlstore.YamlKey('one', 'five'), 'dataz')
        ]
        self.store.save(key_value_pairs)

        assert_equal(self.store.buffer, expected)
        with open(self.filename, 'r') as fh:
            actual = yaml.load(fh)
        assert_equal(actual, expected)






if __name__ == "__main__":
    run()