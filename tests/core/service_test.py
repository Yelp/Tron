import mock
from testify import setup, assert_equal, TestCase, run
from testify.assertions import assert_not_equal
from tests.assertions import assert_mock_calls

from tests.testingutils import autospec_method
from tron.core import service, serviceinstance
from tron import node, command_context, event, eventloop
from tron.core.serviceinstance import ServiceInstance

class ServiceStateTestCase(TestCase):

    @setup
    def setup_service(self):
        self.instances = mock.create_autospec(
            serviceinstance.ServiceInstanceCollection)
        self.service = mock.Mock(enabled=True, instances=self.instances)

    def test_state_disabled(self):
        self.service.enabled = False
        state = service.ServiceState.from_service(self.service)
        assert_equal(state, service.ServiceState.DISABLED)

    def test_state_up(self):
        self.service.enabled = True
        state = service.ServiceState.from_service(self.service)
        assert_equal(state, service.ServiceState.UP)
        self.instances.all.assert_called_with(ServiceInstance.STATE_UP)

    def test_state_degraded(self):
        self.service.enabled = True
        self.instances.all.return_value = False
        self.instances.is_starting.return_value = False
        state = service.ServiceState.from_service(self.service)
        assert_equal(state, service.ServiceState.DEGRADED)


class ServiceTestCase(TestCase):

    @setup
    def setup_service(self):
        self.config = mock.MagicMock()
        self.instances = mock.create_autospec(
            serviceinstance.ServiceInstanceCollection,
            stop=mock.Mock(), start=mock.Mock(), state_data=mock.Mock(),
            restore=mock.Mock())
        self.service = service.Service(self.config, self.instances)
        autospec_method(self.service.watch)
        self.service.repair_callback = mock.create_autospec(
            eventloop.UniqueCallback)

    @mock.patch('tron.core.service.node')
    def test_from_config(self, mock_node):
        node_store = mock.create_autospec(node.NodePoolStore)
        mock_node.NodePoolStore.get_instance.return_value = node_store
        node_store.__getitem__.return_value = mock.create_autospec(node.Node)
        context = mock.create_autospec(command_context.CommandContext)

        service_inst = service.Service.from_config(self.config, context)
        collection = service_inst.instances
        assert_equal(service_inst.config, self.config)
        assert_equal(collection.node_pool, node_store[self.config.node])
        assert_equal(collection.context, context)

    def test_enable(self):
        autospec_method(self.service.repair)
        self.service.enable()
        assert self.service.enabled
        self.service.repair.assert_called_with()

    def test_disable(self):
        self.service.disable()
        assert not self.service.enabled
        self.instances.stop.assert_called_with()
        self.service.repair_callback.cancel.assert_called_with()

    def test_repair(self):
        autospec_method(self.service.notify)
        count = 3
        created_instances = [
            mock.create_autospec(ServiceInstance) for _ in xrange(count)]
        self.instances.create_missing.return_value = created_instances
        self.service.repair()
        self.instances.clear_failed.assert_called_with()
        assert_equal(self.service.watch.mock_calls,
            [mock.call(inst.get_observable(), True) for inst in created_instances])
        self.instances.restore.assert_called_with()
        self.instances.start.assert_called_with()
        self.service.notify.assert_called_with(self.service.NOTIFY_STATE_CHANGE)

    def test_handle_instance_state_change_down(self):
        autospec_method(self.service.notify)
        instance_event = serviceinstance.ServiceInstance.STATE_DOWN
        self.service._handle_instance_state_change(mock.Mock(), instance_event)
        self.service.notify.assert_called_with(self.service.NOTIFY_STATE_CHANGE)
        self.service.instances.clear_down.assert_called_with()

    def test_handle_instance_state_change_failed(self):
        autospec_method(self.service.notify)
        autospec_method(self.service.record_events)
        instance_event = serviceinstance.ServiceInstance.STATE_FAILED
        self.service._handle_instance_state_change(mock.Mock(), instance_event)
        assert not self.service.notify.mock_calls
        self.service.record_events.assert_called_with()

    def test_handle_instance_state_change_starting(self):
        autospec_method(self.service.notify)
        autospec_method(self.service.record_events)
        instance_event = serviceinstance.ServiceInstance.STATE_STARTING
        self.service._handle_instance_state_change(mock.Mock(), instance_event)
        assert not self.service.notify.mock_calls
        assert not self.service.record_events.mock_calls

    def test_record_events_failure(self):
        autospec_method(self.service.get_state)
        state = self.service.get_state.return_value  = service.ServiceState.FAILED
        self.service.event_recorder = mock.create_autospec(event.EventRecorder)
        self.service.record_events()
        self.service.event_recorder.critical.assert_called_with(state)

    def test_record_events_up(self):
        autospec_method(self.service.get_state)
        state = self.service.get_state.return_value  = service.ServiceState.UP
        self.service.event_recorder = mock.create_autospec(event.EventRecorder)
        self.service.record_events()
        self.service.event_recorder.ok.assert_called_with(state)

    def test_state_data(self):
        expected = dict(enabled=False, instances=self.instances.state_data)
        assert_equal(self.service.state_data, expected)

    def test__eq__not_equal(self):
        assert_not_equal(self.service, None)
        assert_not_equal(self.service, mock.Mock())
        other = service.Service(self.config, mock.Mock())
        assert_not_equal(self.service, other)

    def test__eq__(self):
        other = service.Service(self.config, self.instances)
        assert_equal(self.service, other)

    def test_restore_state(self):
        autospec_method(self.service.watch_instances)
        autospec_method(self.service.enable)
        state_data = {'enabled': True, 'instances': []}
        self.service.restore_state(state_data)
        self.service.watch_instances.assert_called_with(
            self.instances.restore_state.return_value)
        self.service.enable.assert_called_with()


class ServiceCollectionTestCase(TestCase):

    @setup
    def setup_collection(self):
        self.collection = service.ServiceCollection()
        self.service_list = [
            mock.create_autospec(service.Service) for _ in xrange(3)]

    def _add_service(self):
        self.collection.services.update(
            (serv.name, serv) for serv in self.service_list)

    @mock.patch('tron.core.service.Service', autospec=True)
    def test_load_from_config(self, mock_service):
        autospec_method(self.collection.get_names)
        autospec_method(self.collection.add)
        service_configs = {'a': mock.Mock(), 'b': mock.Mock()}
        context = mock.create_autospec(command_context.CommandContext)
        result = list(self.collection.load_from_config(service_configs, context))
        expected = [mock.call(config, context)
                    for config in service_configs.itervalues()]
        assert_mock_calls(expected, mock_service.from_config.mock_calls)
        expected = [mock.call(s) for s in result]
        assert_mock_calls(expected, self.collection.add.mock_calls)

    def test_add(self):
        self.collection.services = mock.MagicMock()
        service = mock.Mock()
        result = self.collection.add(service)
        self.collection.services.add.assert_called_with(
            service, self.collection.remove_by_service)
        assert_equal(result, self.collection.services.add.return_value)

    def test_restore_state(self):
        state_count = 2
        state_data = dict(
            (serv.name, serv) for serv in self.service_list[:state_count])
        self._add_service()
        self.collection.restore_state(state_data)
        for name in state_data:
            service = self.collection.services[name]
            service.restore_state.assert_called_with(state_data[name])


if __name__ == "__main__":
    run()
