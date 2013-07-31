import contextlib
import mock
import signal
from Queue import Queue, Empty
from testify import TestCase, assert_equal, assert_raises, setup_teardown, setup, run

from tron.serialize.runstate.tronstore import tronstore, msg_enums

class TronstoreMainTestCase(TestCase):

	@setup_teardown
	def setup_main(self):
		self.config           = mock.Mock()
		self.pipe             = mock.Mock()
		self.store_class      = mock.Mock()
		self.thread_pool      = mock.Mock()
		self.request_factory  = mock.Mock()
		self.response_factory = mock.Mock()

		def echo_single_request(request):
			return request
		self.request_factory.from_msg = echo_single_request

		def echo_requests(not_used):
			return self.requests

		def raise_to_exit(exitcode):
			raise SystemError

		with contextlib.nested(
			mock.patch('tron.serialize.runstate.tronstore.store.SyncStore',
				new=mock.Mock(return_value=self.store_class)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.TronstorePool',
				new=mock.Mock(return_value=self.thread_pool)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.SyncPipe',
				new=mock.Mock(return_value=self.pipe)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.StoreRequestFactory',
				new=mock.Mock(return_value=self.request_factory)),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.StoreResponseFactory',
				new=mock.Mock(return_value=self.response_factory)),
			mock.patch.object(tronstore.os, '_exit',
				autospec=True)
		) as (
			self.store_patch,
			self.thread_patch,
			self.pipe_patch,
			self.request_patch,
			self.response_patch,
			self.exit_patch
		):
			self.main = tronstore.TronstoreMain(self.config, self.pipe)
			yield

	def test__init__(self):
		self.store_patch.assert_called_once_with(self.config)
		self.request_patch.assert_called_once_with()
		self.pipe_patch.assert_called_once_with(self.pipe)
		self.response_patch.assert_called_once_with()
		self.thread_patch.assert_called_once_with(self.response_factory, self.pipe, self.store_class)
		assert_equal(self.main.config, self.config)
		assert not self.main.is_shutdown
		assert not self.main.shutdown_req_id

	def test_get_all_from_pipe(self):
		fake_data = 'fuego'
		self.pipe.recv_bytes = mock.Mock(return_value=fake_data)
		self.pipe.poll = mock.Mock(side_effect=iter([True, False]))
		assert_equal(self.main._get_all_from_pipe(), [fake_data])
		self.pipe.recv_bytes.assert_called_once_with()
		assert_equal(self.pipe.poll.call_count, 2)

	def test_reconfigure_success(self):
		fake_id = 77
		fake_data = mock.Mock()
		request = mock.Mock(req_type=msg_enums.REQUEST_CONFIG, id=fake_id, data=fake_data)

		self.main._reconfigure(request)
		self.thread_pool.stop.assert_called_once_with()
		self.thread_pool.start.assert_called_once_with()
		self.store_class.cleanup.assert_called_once_with()
		self.store_patch.assert_any_call(fake_data)
		self.thread_patch.assert_any_call(self.response_factory, self.pipe, self.store_class)
		assert_equal(self.thread_patch.call_count, 2)
		assert_equal(self.main.config, fake_data)
		self.response_factory.build.assert_called_once_with(True, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.response_factory.build().serialized)

	def test_reconfigure_failure(self):
		fake_id = 77
		fake_data = mock.Mock()
		request = mock.Mock(req_type=msg_enums.REQUEST_CONFIG, id=fake_id, data=fake_data)
		self.store_patch.configure_mock(side_effect=iter([SystemError, lambda x: None]))

		self.main._reconfigure(request)
		assert_equal(self.store_patch.call_count, 3)
		self.store_patch.assert_any_call(fake_data)
		self.store_patch.assert_any_call(self.config)
		self.thread_patch.assert_any_call(self.response_factory, self.pipe,
			self.store_class)
		assert_equal(self.thread_patch.call_count, 2)
		self.thread_pool.stop.assert_called_once_with()
		self.store_class.cleanup.assert_called_once_with()
		self.thread_pool.start.assert_called_once_with()
		self.response_factory.build.assert_called_once_with(False, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.response_factory.build().serialized)

	def test_handle_request_save(self):
		fake_id = 77
		request = mock.Mock(req_type=msg_enums.REQUEST_SAVE, id=fake_id)
		self.main._handle_request(request)
		self.thread_pool.enqueue_work.assert_called_once_with(request)

	def test_handle_request_restore(self):
		fake_id = 77
		request = mock.Mock(req_type=msg_enums.REQUEST_RESTORE, id=fake_id)
		self.main._handle_request(request)
		self.thread_pool.enqueue_work.assert_called_once_with(request)

	def test_handle_request_shutdown(self):
		fake_id = 77
		request = mock.Mock(req_type=msg_enums.REQUEST_SHUTDOWN, id=fake_id)
		self.main._handle_request(request)
		assert self.main.is_shutdown
		assert_equal(fake_id, self.main.shutdown_req_id)

	def test_handle_request_config(self):
		fake_id = 77
		request = mock.Mock(req_type=msg_enums.REQUEST_CONFIG, id=fake_id)
		with mock.patch.object(self.main, '_reconfigure') as reconf_patch:
			self.main._handle_request(request)
			reconf_patch.assert_called_once_with(request)

	def test_shutdown_has_id(self):
		fake_id = 77
		self.main.shutdown_req_id = fake_id
		self.main._shutdown()
		self.thread_pool.stop.assert_called_once_with()
		self.store_class.cleanup.assert_called_once_with()
		self.response_factory.build.assert_called_once_with(True, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.response_factory.build().serialized)
		self.exit_patch.assert_called_once_with(0)

	def test_shutdown_no_id(self):
		self.main.shutdown_req_id = None
		self.main._shutdown()
		self.thread_pool.stop.assert_called_once_with()
		self.store_class.cleanup.assert_called_once_with()
		assert not self.response_factory.build.called
		assert not self.pipe.send_bytes.called
		self.exit_patch.assert_called_once_with(0)

	def test_main_loop_handle_requests(self):
		self.main.is_shutdown = False
		self.pipe.poll = mock.Mock(return_value=True)
		requests = [mock.Mock(), mock.Mock()]
		self.request_factory.from_msg = mock.Mock(side_effect=lambda x: x)
		with contextlib.nested(
			mock.patch.object(self.main, '_get_all_from_pipe', return_value=requests),
			mock.patch.object(self.main, '_handle_request',
				side_effect=iter([None, SystemError]))
		) as (all_patch, handle_patch):
			assert_raises(SystemError, self.main.main_loop)
			self.thread_pool.start.assert_called_once_with()
			self.pipe.poll.assert_called_once_with(self.main.POLL_TIMEOUT)
			all_patch.assert_called_once_with()
			self.request_factory.from_msg.assert_has_calls(
				[mock.call(requests[i]) for i in xrange(len(requests))])
			handle_patch.assert_has_calls(
				[mock.call(requests[i]) for i in xrange(len(requests))])

	def test_main_loop_is_shutdown(self):
		self.main.is_shutdown = True
		self.pipe.poll.configure_mock(return_value=False)
		with mock.patch.object(self.main, '_shutdown',
		side_effect=SystemError) as shutdown_patch:
			assert_raises(SystemError, self.main.main_loop)
			self.thread_pool.start.assert_called_once_with()
			self.pipe.poll.assert_called_once_with(self.main.POLL_TIMEOUT)
			shutdown_patch.assert_called_once_with()

	def test_main_loop_trond_check(self):
		fake_id = 77
		self.main.is_shutdown = False
		self.pipe.poll = mock.Mock(side_effect=iter([False, SystemError]))
		with contextlib.nested(
			mock.patch.object(tronstore.os, 'kill', side_effect=TypeError),
			mock.patch.object(tronstore.os, 'getppid', return_value=fake_id)
		) as (kill_patch, ppid_patch):
			assert_raises(SystemError, self.main.main_loop)
			assert self.main.is_shutdown
			ppid_patch.assert_called_once_with()
			kill_patch.assert_called_once_with(fake_id, 0)
			assert_equal(self.pipe.poll.call_count, 2)


class TronstoreHandleRequestsTestCase(TestCase):

	@setup
	def setup_args(self):
		self.queue       = mock.Mock()
		self.store_class = mock.Mock()
		self.pipe        = mock.Mock()
		self.factory     = mock.Mock()
		self.do_work     = mock.Mock(val=False)

		self.queue.empty.configure_mock(side_effect=iter([False, True]))

	def test_handle_requests_save(self):
		fake_id = 3090
		request_data = ('fantastic', 'voyage')
		data_type = 'lakeside'
		request = mock.Mock(req_type=msg_enums.REQUEST_SAVE, data=request_data,
			data_type=data_type, id=fake_id)
		self.queue.get.configure_mock(return_value=request)

		tronstore.handle_requests(self.queue, self.factory, self.pipe,
			self.store_class, self.do_work)

		self.store_class.save.assert_called_once_with(request_data[0], request_data[1],
			data_type)
		assert_equal(self.queue.empty.call_count, 2)
		self.queue.get.assert_called_once_with(block=True, timeout=1.0)

	def test_handle_requests_restore(self):
		fake_id = 53045
		request_data = 'edgeworth'
		fake_success = ('steel_samurai_fan', 'or_maybe_its_ironic')
		data_type = 'lawyer'
		self.store_class.restore = mock.Mock(return_value=fake_success)
		request = mock.Mock(req_type=msg_enums.REQUEST_RESTORE, data=request_data,
			data_type=data_type, id=fake_id)
		self.queue.get.configure_mock(return_value=request)

		tronstore.handle_requests(self.queue, self.factory, self.pipe,
			self.store_class, self.do_work)

		self.store_class.restore.assert_called_once_with(request_data, data_type)
		self.factory.build.assert_called_once_with(fake_success[0], fake_id, fake_success[1])
		self.pipe.send_bytes.assert_called_once_with(self.factory.build().serialized)
		assert_equal(self.queue.empty.call_count, 2)
		self.queue.get.assert_called_once_with(block=True, timeout=1.0)

	def test_handle_requests_other(self):
		fake_id = 1234567890
		request = mock.Mock(req_type='not_actually_a_request', id=fake_id)
		self.queue.get.configure_mock(return_value=request)

		tronstore.handle_requests(self.queue, self.factory, self.pipe,
			self.store_class, self.do_work)

		self.factory.build.assert_called_once_with(False, fake_id, '')
		self.pipe.send_bytes.assert_called_once_with(self.factory.build().serialized)
		assert_equal(self.queue.empty.call_count, 2)
		self.queue.get.assert_called_once_with(block=True, timeout=1.0)

	def test_handle_requests_cont_on_empty(self):
		self.queue.get.configure_mock(side_effect=Empty)

		tronstore.handle_requests(self.queue, self.factory, self.pipe,
			self.store_class, self.do_work)

		assert_equal(self.queue.empty.call_count, 2)
		self.queue.get.assert_called_once_with(block=True, timeout=1.0)
		assert not self.pipe.send_bytes.called


class TronstoreOtherTestCase(TestCase):

	def test_main(self):
		config = mock.Mock()
		pipe = mock.Mock()
		with contextlib.nested(
			mock.patch.object(tronstore, '_register_null_handlers'),
			mock.patch('tron.serialize.runstate.tronstore.tronstore.TronstoreMain', autospec=True)
		) as (handler_patch, tronstore_patch):
			tronstore.main(config, pipe)
			handler_patch.assert_called_once_with()
			tronstore_patch.assert_called_once_with(config, pipe)
			tronstore_patch.return_value.main_loop.assert_called_once_with()

	def test_register_null_handlers(self):
		with mock.patch.object(tronstore.signal, 'signal') as signal_patch:
			tronstore._register_null_handlers()
			signal_patch.assert_any_call(signal.SIGINT, tronstore._discard_signal)
			signal_patch.assert_any_call(signal.SIGTERM, tronstore._discard_signal)
			signal_patch.assert_any_call(signal.SIGHUP, tronstore._discard_signal)


class PoolBoolTestCase(TestCase):

	def test__init__(self):
		poolbool = tronstore.PoolBool()
		assert poolbool._val
		assert poolbool.val
		assert poolbool.value

	def test__init__invalid(self):
		assert_raises(TypeError, tronstore.PoolBool, 'frue')

	def test__init__false(self):
		poolbool = tronstore.PoolBool(False)
		assert not poolbool._val
		assert not poolbool.val
		assert not poolbool.value

	def test_set(self):
		poolbool = tronstore.PoolBool(False)
		poolbool.set(True)
		assert poolbool._val
		assert poolbool.val
		assert poolbool.value

	def test_set_invalid(self):
		poolbool = tronstore.PoolBool(False)
		assert_raises(TypeError, poolbool.set, 'tralse')
		assert not poolbool._val
		assert not poolbool.val
		assert not poolbool.value


class SyncPipeTestCase(TestCase):

	@setup
	def setup_pipe(self):
		self.pipe = mock.Mock()
		self.sync = tronstore.SyncPipe(self.pipe)
		self.sync.lock = mock.MagicMock()

	def test__init__(self):
		assert_equal(self.pipe, self.sync.pipe)

	def test_poll(self):
		fake_arg = 'arrrrrrg'
		fake_kwarg = 'no_dont_do_it_nishbot_we_love_you'
		self.sync.poll(fake_arg, fake_kwarg=fake_kwarg)
		self.pipe.poll.assert_called_once_with(fake_arg, fake_kwarg=fake_kwarg)
		assert not self.sync.lock.__enter__.called
		assert not self.sync.lock.lock.called

	def test_send_bytes(self):
		fake_arg = 'makin_bacon'
		fake_kwarg = 'hioh_its_mnc'
		fake_return = 'churros'
		self.pipe.send_bytes.configure_mock(return_value=fake_return)
		assert_equal(self.sync.send_bytes(fake_arg, fake_kwarg=fake_kwarg), fake_return)
		self.pipe.send_bytes.assert_called_once_with(fake_arg, fake_kwarg=fake_kwarg)
		self.sync.lock.__enter__.assert_called_once_with()
		self.sync.lock.__exit__.assert_called_once_with(None, None, None)

	def test_recv_bytes(self):
		fake_arg = 'hey_can_i_have_root'
		fake_kwarg = 'pls'
		fake_return = 'PPFFFFFFFFAHAHAHAHAHHA'
		self.pipe.recv_bytes.configure_mock(return_value=fake_return)
		assert_equal(self.sync.recv_bytes(fake_arg, fake_kwarg=fake_kwarg), fake_return)
		self.pipe.recv_bytes.assert_called_once_with(fake_arg, fake_kwarg=fake_kwarg)
		self.sync.lock.__enter__.assert_called_once_with()
		self.sync.lock.__exit__.assert_called_once_with(None, None, None)


class TronstorePoolTestCase(TestCase):

	@setup_teardown
	def setup_tronstore_pool(self):
		self.factory = mock.Mock()
		self.pipe = mock.Mock()
		self.store = mock.Mock()
		with mock.patch('tron.serialize.runstate.tronstore.tronstore.Thread', autospec=True) \
		as self.thread_patch:
			self.pool = tronstore.TronstorePool(self.factory, self.pipe, self.store)
			yield

	def test__init__(self):
		assert isinstance(self.pool.request_queue, tronstore.Queue)
		assert_equal(self.pool.response_factory, self.factory)
		assert_equal(self.pool.pipe, self.pipe)
		assert_equal(self.pool.store_class, self.store)
		assert isinstance(self.pool.keep_working, tronstore.PoolBool)
		assert self.pool.keep_working.value
		assert_equal(self.pool.thread_pool, [self.thread_patch.return_value for i in range(self.pool.POOL_SIZE)])
		self.thread_patch.assert_any_call(target=tronstore.handle_requests,
			args=(
				self.pool.request_queue,
				self.factory,
				self.pipe,
				self.store,
				self.pool.keep_working
			))

	def test_start(self):
		self.pool.keep_working.set(False)
		self.pool.start()
		assert self.pool.keep_working.value
		assert not self.thread_patch.return_value.daemon
		assert_equal(self.thread_patch.return_value.start.call_count, self.pool.POOL_SIZE)

	def test_stop(self):
		self.pool.keep_working.set(True)
		self.thread_patch.return_value.is_alive.return_value = False
		with mock.patch.object(self.pool, 'has_work', return_value=False) \
		as work_patch:
			self.pool.stop()
			assert not self.pool.keep_working.value
			work_patch.assert_called_once_with()
			assert_equal(self.thread_patch.return_value.is_alive.call_count, self.pool.POOL_SIZE)

	def test_enqueue_work(self):
		fake_work = 'youre_fired'
		with mock.patch.object(self.pool.request_queue, 'put') as put_patch:
			self.pool.enqueue_work(fake_work)
			put_patch.assert_called_once_with(fake_work)

	def test_has_work(self):
		with mock.patch.object(self.pool.request_queue, 'empty', return_value=True) \
		as empty_patch:
			assert not self.pool.has_work()
			empty_patch.assert_called_once_with()


if __name__ == "__main__":
	run()
