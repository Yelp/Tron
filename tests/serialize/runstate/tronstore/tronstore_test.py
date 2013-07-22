import contextlib
import mock
import signal
from Queue import Queue
from testify import TestCase, assert_equal, assert_raises, setup_teardown, setup

from tron.serialize.runstate.tronstore import tronstore, msg_enums

class TronstoreMainTestCase(TestCase):

	@setup_teardown
	def setup_main(self):
		self.config           = mock.Mock()
		self.pipe             = mock.Mock()
		self.store_class      = mock.Mock()
		self.trans_method     = mock.Mock()
		self.mock_thread      = mock.Mock(is_alive=lambda: False)
		self.request_factory  = mock.Mock()
		self.response_factory = mock.Mock()
		self.lock             = mock.Mock()
		self.queue            = mock.Mock()

		def poll_patch(timeout):
			return False if tronstore.is_shutdown else True
		self.pipe.poll = poll_patch

		def echo_single_request(request):
			return request
		self.request_factory.rebuild = echo_single_request

		def echo_requests(not_used):
			return self.requests

		def raise_to_exit(exitcode):
			raise SystemError

		with contextlib.nested(
			mock.patch.object(tronstore, 'parse_config',
				return_value=(self.store_class, self.trans_method)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.Thread',
				new=mock.Mock(return_value=self.mock_thread)),
			mock.patch.object(signal, 'signal'),
			mock.patch.object(tronstore, 'get_all_from_pipe',
				side_effect=echo_requests),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.StoreRequestFactory',
				new=mock.Mock(return_value=self.request_factory)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.StoreResponseFactory',
				new=mock.Mock(return_value=self.response_factory)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.Lock',
				new=mock.Mock(return_value=self.lock)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.Queue',
				new=mock.Mock(return_value=self.queue)),
			mock.patch.object(tronstore.os, '_exit',
				side_effect=raise_to_exit)
		) as (
			self.parse_patch,
			self.thread_patch,
			self.signal_patch,
			self.get_all_patch,
			self.request_patch,
			self.response_patch,
			self.lock_patch,
			self.queue_patch,
			self.exit_patch
		):
			yield

	def assert_main_startup(self):
		self.parse_patch.assert_any_call(self.config)
		self.request_patch.assert_called_once_with(self.trans_method)
		self.response_patch.assert_called_once_with(self.trans_method)
		assert_equal(self.lock_patch.call_count, 2)
		self.queue_patch.assert_called_once_with()
		self.thread_patch.assert_any_call(target=tronstore.thread_starter, args=(self.queue, []))
		self.mock_thread.start.assert_called_once_with()
		self.signal_patch.assert_any_call(signal.SIGINT, tronstore._discard_signal)
		self.signal_patch.assert_any_call(signal.SIGHUP, tronstore._discard_signal)
		self.signal_patch.assert_any_call(signal.SIGTERM, tronstore._discard_signal)
		self.exit_patch.assert_called_once_with(0)

	def test_shutdown_request(self):
		fake_id = 77
		self.requests = [mock.Mock(req_type=msg_enums.REQUEST_SHUTDOWN, id=fake_id)]
		assert_raises(SystemError, tronstore.main, self.config, self.pipe)

		self.assert_main_startup()
		assert tronstore.is_shutdown
		self.get_all_patch.assert_called_once_with(self.pipe)
		self.store_class.cleanup.assert_called_once_with()
		self.response_factory.build.assert_called_once_with(True, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.response_factory.build().serialized)

	def test_config_request_success(self):
		fake_id = 77
		fake_shutdown_id = 88
		self.requests = [mock.Mock(req_type=msg_enums.REQUEST_CONFIG, id=fake_id),
			mock.Mock(req_type=msg_enums.REQUEST_SHUTDOWN, id=fake_shutdown_id)]
		assert_raises(SystemError, tronstore.main, self.config, self.pipe)

		self.assert_main_startup()
		assert tronstore.is_shutdown
		assert_equal(self.store_class.cleanup.call_count, 2)
		self.store_class.cleanup.assert_any_call()
		assert_equal(self.parse_patch.call_count, 2)
		self.queue.empty.assert_called_once_with()
		self.response_factory.build.assert_any_call(True, fake_id, '')
		self.pipe.send_bytes.assert_any_call(self.response_factory.build().serialized)
		self.request_factory.update_method.assert_called_once_with(self.trans_method)
		self.response_factory.update_method.assert_called_once_with(self.trans_method)

	def test_config_request_exception(self):
		fake_id = 77
		fake_shutdown_id = 88
		some_fake_store = mock.Mock()
		some_fake_trans = mock.Mock()
		self.requests = [mock.Mock(req_type=msg_enums.REQUEST_CONFIG, id=fake_id),
			mock.Mock(req_type=msg_enums.REQUEST_SHUTDOWN, id=fake_shutdown_id)]
		item_iter = iter([(self.store_class, self.trans_method), 'breakit',
			(some_fake_store, some_fake_trans)])
		self.parse_patch.configure_mock(side_effect=item_iter)
		assert_raises(SystemError, tronstore.main, self.config, self.pipe)

		self.assert_main_startup()
		assert tronstore.is_shutdown
		self.store_class.cleanup.assert_called_once_with()
		some_fake_store.cleanup.assert_called_once_with()
		assert_equal(self.parse_patch.call_count, 3)
		self.queue.empty.assert_called_once_with()
		self.response_factory.build.assert_any_call(False, fake_id, '')
		self.pipe.send_bytes.assert_any_call(self.response_factory.build().serialized)
		self.request_factory.update_method.assert_called_once_with(some_fake_trans)
		self.response_factory.update_method.assert_called_once_with(some_fake_trans)

	def test_save_request(self):
		fake_id = 77
		fake_shutdown_id = 88
		self.requests = [mock.Mock(req_type=msg_enums.REQUEST_SAVE, id=fake_id),
			mock.Mock(req_type=msg_enums.REQUEST_SHUTDOWN, id=fake_shutdown_id)]
		assert_raises(SystemError, tronstore.main, self.config, self.pipe)

		self.assert_main_startup()
		assert tronstore.is_shutdown
		self.thread_patch.assert_any_call(target=tronstore.handle_request,
			args=(
				self.requests[0],
				self.store_class,
				self.pipe,
				self.response_factory,
				self.lock,
				self.lock))
		self.queue.put.assert_called_once_with(self.mock_thread)

	def test_restore_request(self):
		fake_id = 77
		fake_shutdown_id = 88
		self.requests = [mock.Mock(req_type=msg_enums.REQUEST_RESTORE, id=fake_id),
			mock.Mock(req_type=msg_enums.REQUEST_SHUTDOWN, id=fake_shutdown_id)]
		assert_raises(SystemError, tronstore.main, self.config, self.pipe)

		self.assert_main_startup()
		assert tronstore.is_shutdown
		self.thread_patch.assert_any_call(target=tronstore.handle_request,
			args=(
				self.requests[0],
				self.store_class,
				self.pipe,
				self.response_factory,
				self.lock,
				self.lock))
		self.queue.put.assert_called_once_with(self.mock_thread)


class TronstoreHandleRequestTestCase(TestCase):

	@setup
	def setup_args(self):
		self.store_class  = mock.Mock()
		self.pipe         = mock.Mock()
		self.factory      = mock.Mock()
		self.save_lock    = mock.MagicMock()
		self.restore_lock = mock.MagicMock()

	def test_handle_request_save(self):
		fake_id = 3090
		request_data = ('fantastic', 'voyage')
		data_type = 'lakeside'
		fake_success = 'eaten_by_a_gru'
		self.store_class.save = mock.Mock(return_value=fake_success)
		request = mock.Mock(req_type=msg_enums.REQUEST_SAVE, data=request_data,
			data_type=data_type, id=fake_id)

		tronstore.handle_request(request, self.store_class, self.pipe,
			self.factory, self.save_lock, self.restore_lock)

		self.save_lock.__enter__.assert_called_once_with()
		self.save_lock.__exit__.assert_called_once_with(None, None, None)
		self.store_class.save.assert_called_once_with(request_data[0], request_data[1],
			data_type)
		self.factory.build.assert_called_once_with(fake_success, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.factory.build().serialized)

	def test_handle_request_restore(self):
		fake_id = 53045
		request_data = 'edgeworth'
		fake_success = ('steel_samurai_fan', 'or_maybe_its_ironic')
		data_type = 'lawyer'
		self.store_class.restore = mock.Mock(return_value=fake_success)
		request = mock.Mock(req_type=msg_enums.REQUEST_RESTORE, data=request_data,
			data_type=data_type, id=fake_id)

		tronstore.handle_request(request, self.store_class, self.pipe,
			self.factory, self.save_lock, self.restore_lock)

		self.restore_lock.__enter__.assert_called_once_with()
		self.restore_lock.__exit__.assert_called_once_with(None, None, None)
		self.store_class.restore.assert_called_once_with(request_data, data_type)
		self.factory.build.assert_called_once_with(fake_success[0], fake_id, fake_success[1])
		self.pipe.send_bytes.assert_called_once_with(self.factory.build().serialized)

	def test_handle_request_other(self):
		fake_id = 1234567890
		request = mock.Mock(req_type='not_actually_a_request', id=fake_id)

		tronstore.handle_request(request, self.store_class, self.pipe,
			self.factory, self.save_lock, self.restore_lock)

		self.factory.build.assert_called_once_with(False, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.factory.build().serialized)


class TronstoreOtherTestCase(TestCase):

	def test_parse_config(self):
		fake_config = mock.Mock(
			name='yo_earl',
			transport_method='what',
			store_type='you\'re fired',
			connection_details='HNNNNNNLLLLLGGG',
			db_store_method='one_too_many_lines')
		fake_store = 'lady_madonna'
		with mock.patch.object(tronstore.store, 'build_store',
		return_value=fake_store) as build_patch:
			assert_equal(tronstore.parse_config(fake_config), (fake_store, fake_config.transport_method))
			build_patch.assert_called_once_with(
				fake_config.name,
				fake_config.store_type,
				fake_config.connection_details,
				fake_config.db_store_method)

	def test_get_all_from_pipe(self):
		fake_data = 'fuego'
		pipe = mock.Mock()
		pipe.recv_bytes = mock.Mock(return_value=fake_data)
		pipe.poll = mock.Mock(side_effect=iter([True, False]))
		assert_equal(tronstore.get_all_from_pipe(pipe), [fake_data])
		pipe.recv_bytes.assert_called_once_with()
		assert_equal(pipe.poll.call_count, 2)


class TronstoreThreadStarterTestCase(TestCase):

	@setup_teardown
	def setup_thread_starter(self):
		tronstore.is_shutdown = False
		with mock.patch.object(tronstore.time, 'sleep') as self.sleep_patch:
			yield

	def test_pool_size_limit(self):
		fake_thread = mock.Mock()
		fake_queue = Queue()
		map(fake_queue.put, [fake_thread for i in range(tronstore.POOL_SIZE + 1)])
		fake_thread.is_alive = lambda: not self.sleep_patch.called
		running_threads = []

		def shutdown_tronstore(time):
			tronstore.is_shutdown = True
		self.sleep_patch.configure_mock(side_effect=shutdown_tronstore)

		tronstore.thread_starter(fake_queue, running_threads)
		assert_equal(running_threads, [])
		assert_equal(fake_thread.start.call_count, tronstore.POOL_SIZE+1)
		self.sleep_patch.assert_called_once_with(0.5)
		assert fake_queue.empty()
		tronstore.is_shutdown = False  # to make sure nothing weird happens

	def test_shutdown_condition(self):
		fake_queue = Queue()
		running_threads = []
		tronstore.is_shutdown = True

		with mock.patch.object(tronstore, '_remove_finished_threads',
		return_value=0) as remove_patch:
			tronstore.thread_starter(fake_queue, running_threads)
			assert not remove_patch.called
			assert not self.sleep_patch.called
			assert_equal(running_threads, [])
		tronstore.is_shutdown = False

	def test_running_thread_operations(self):
		fake_queue = Queue()
		fake_thread = mock.Mock()
		running_threads = mock.Mock(__len__=lambda i: 0)

		def shutdown_tronstore(time):
			tronstore.is_shutdown = True
		running_threads.append = mock.Mock(side_effect=shutdown_tronstore)

		tronstore.is_shutdown = False
		fake_queue.put(fake_thread)

		with mock.patch.object(tronstore, '_remove_finished_threads',
		return_value=0) as remove_patch:
			tronstore.thread_starter(fake_queue, running_threads)
			remove_patch.assert_called_once_with(running_threads)
			fake_thread.start.assert_called_once_with()
			running_threads.append.assert_called_once_with(fake_thread)

	def test_remove_finished_threads(self):
		fake_thread = mock.Mock(is_alive=mock.Mock(return_value=False))
		fake_get = mock.Mock(return_value=fake_thread)
		fake_pop = mock.Mock()
		fake_len = 5
		running_threads = mock.Mock(
			__len__=lambda i: fake_len,
			__getitem__=fake_get,
			pop=fake_pop)
		assert_equal(tronstore._remove_finished_threads(running_threads), 5)
		calls = [mock.call(i) for i in range(fake_len-1, -1, -1)]
		fake_get.assert_has_calls(calls)
		fake_pop.assert_has_calls(calls)
		assert_equal(fake_thread.is_alive.call_count, 5)
