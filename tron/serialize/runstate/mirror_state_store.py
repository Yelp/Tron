import copy
from pprint import pformat
from threading import Thread


class MirrorStateStore():
    def __init__(self, shelve_store, dynamodb_store) -> None:
        self.shelve_store = shelve_store
        self.dynamodb_store = dynamodb_store

    def is_valid(self, shelve_kv_pairs: dict, dynamo_kv_pairs: dict, diffs: list) -> bool:
        """
        It checks if all keys in DynamoDB are in shelveStateStore and have the same values.
        It is possible that some keys are in shelveStateStore but is not in DynamoDB when the
        migration starts.
        """
        count = 0
        for k, v in shelve_kv_pairs.items():
            if k in dynamo_kv_pairs.keys():
                count += 1
                dynamo_diff = pformat(dynamo_kv_pairs[k])
                shelve_diff = pformat(shelve_kv_pairs[k])
                if dynamo_diff != shelve_diff:
                    diffs.append(dynamo_diff)
                    diffs.append(shelve_diff)
        return count == len(dynamo_kv_pairs) and not diffs

    def build_key(self, type, iden):
        """
        It builds a unique partition key. The key could be objects with __str__ method.
        """
        return self.shelve_store.build_key(type, iden)

    def save(self, key_value_pairs) -> None:
        """
        Remove all previous data with the same partition key, examine the size of state_data,
        and splice it into different parts under 400KB with different sort keys,
        and save them under the same partition key built.
        """
        key_value_pairs = list(key_value_pairs)
        thread1 = Thread(target = self.shelve_store.save, args=(copy.deepcopy(key_value_pairs),))
        thread2 = Thread(target = self.dynamodb_store.save, args=(key_value_pairs,))
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

    def restore(self, keys) -> dict:
        """
        Fetch all under the same parition key(keys).
        ret: <dict of key to states>
        """
        keys = list(keys)

        def shelve_restore(keys, res):
            res.update(self.shelve_store.restore(keys))

        def dynamodb_restore(keys, res):
            res.update(self.dynamodb_store.restore(keys))

        dynamo_kv_pairs, shelve_kv_pairs = {}, {}
        thread1 = Thread(target = dynamodb_restore, args=(copy.deepcopy(keys), dynamo_kv_pairs,))
        thread2 = Thread(target = shelve_restore, args=(keys, shelve_kv_pairs,))
        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        diffs = []
        if not self.is_valid(shelve_kv_pairs, dynamo_kv_pairs, diffs):
            msg = 'Data in dynamoDB is not synced to BerkleyDB. This is not critical' \
                  + 'since only BerkleyDB is used to restore states right now'
            msg += '\n\n'.join(diffs)
            self.dynamodb_store.alert(msg)

        return shelve_kv_pairs

    def cleanup(self) -> None:
        self.shelve_store.cleanup()
