import contextlib
import mock
import signal
import os
from testify import TestCase, assert_equal, assert_raises, setup_teardown
from tron.serialize.runstate.tronstore import tronstore
from tron.serialize.runstate.tronstore.process import StoreProcessProtocol, TronStoreError

class StoreProcessProtocolTestCase(TestCase):

	@setup_teardown
	def setup_process(self):
		self.test_pipe_a = mock.Mock()
		self.test_pipe_b = mock.Mock()
		pipe_return = mock.Mock(return_value=(self.test_pipe_a, self.test_pipe_b))
		with contextlib.nested(
			mock.patch('tron.serialize.runstate.tronstore.process.Process',
				autospec=True),
			mock.patch('tron.serialize.runstate.tronstore.process.Pipe',
				new=pipe_return)
		) as (self.process_patch, self.pipe_setup_patch):
			self.config = mock.Mock(
				name='test_config',
				transport_method='pickle',
				store_type='shelve',
				connection_details=None,
				db_store_method=None,
				buffer_size=1
			)
			self.factory = mock.Mock()
			self.process = StoreProcessProtocol(self.config, self.factory)
			yield

	def test__init__(self):
		assert_equal(self.process.response_factory, self.factory)
		assert_equal(self.process.config, self.config)
		assert_equal(self.process.orphaned_responses, {})
		assert not self.process.is_shutdown

	def test_start_process(self):
		self.pipe_setup_patch.assert_called_once_with()
		self.process_patch.assert_called_once_with(target=tronstore.main, args=(self.config, self.test_pipe_b))
		assert self.process_patch.daemon
		self.process.process.start.assert_called_once_with()

	def test_verify_is_alive_while_dead(self):
		with contextlib.nested(
			mock.patch.object(self.process.process, 'is_alive', return_value=False),
			mock.patch.object(self.process, '_start_process'),
		) as (alive_patch, start_patch):
			assert_raises(TronStoreError, self.process._verify_is_alive)
			alive_patch.assert_called_with()
			assert_equal(alive_patch.call_count, 2)
			start_patch.assert_called_once_with()

	def test_verify_is_alive_while_alive(self):
		with contextlib.nested(
			mock.patch.object(self.process.process, 'is_alive', return_value=True),
			mock.patch.object(self.process, '_start_process'),
		) as (alive_patch, start_patch):
			self.process._verify_is_alive()
			alive_patch.assert_called_once_with()
			assert not start_patch.called

	def test_send_request_running(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='sunny_sausalito', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process, '_verify_is_alive'),
			mock.patch.object(self.process.pipe, 'send_bytes')
		) as (verify_patch, pipe_patch):
			self.process.send_request(test_request)
			verify_patch.assert_called_once_with()
			pipe_patch.assert_called_once_with(test_request.serialized)

	def test_send_request_shutdown(self):
		self.process.is_shutdown = True
		fake_id = 77
		test_request = mock.Mock(serialized='whiskey_media', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process, '_verify_is_alive'),
			mock.patch.object(self.process.pipe, 'send_bytes')
		) as (verify_patch, pipe_patch):
			self.process.send_request(test_request)
			assert not verify_patch.called
			assert not pipe_patch.called

	def test_send_request_get_response_running_with_response(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='objection', id=fake_id)
		test_response = mock.Mock(id=fake_id, data='overruled', success=True)
		with contextlib.nested(
			mock.patch.object(self.process, '_verify_is_alive'),
			mock.patch.object(self.process.pipe, 'send_bytes'),
			mock.patch.object(self.process, '_poll_for_response', return_value=test_response)
		) as (verify_patch, pipe_patch, poll_patch):
			assert_equal(self.process.send_request_get_response(test_request), test_response)
			verify_patch.assert_called_once_with()
			pipe_patch.assert_called_once_with(test_request.serialized)
			poll_patch.assert_called_once_with(fake_id, self.process.POLL_TIMEOUT)

	def test_send_request_get_response_running_no_response(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='maaaaaagiiiiccc', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process, '_verify_is_alive'),
			mock.patch.object(self.process.pipe, 'send_bytes'),
			mock.patch.object(self.process, '_poll_for_response', return_value=None)
		) as (verify_patch, pipe_patch, poll_patch):
			assert_equal(self.process.send_request_get_response(test_request),
				self.process.response_factory.build(False, fake_id, ''))
			verify_patch.assert_called_once_with()
			pipe_patch.assert_called_once_with(test_request.serialized)
			poll_patch.assert_called_once_with(fake_id, self.process.POLL_TIMEOUT)

	def test_send_request_get_response_shutdown(self):
		self.process.is_shutdown = True
		fake_id = 77
		test_request = mock.Mock(serialized='i_wish_for_the_nile', id=fake_id)
		test_response = mock.Mock(id=fake_id, data='no_way', success=True)
		with contextlib.nested(
			mock.patch.object(self.process, '_verify_is_alive'),
			mock.patch.object(self.process.pipe, 'send_bytes'),
			mock.patch.object(self.process, '_poll_for_response', return_value=test_response)
		) as (verify_patch, pipe_patch, poll_patch):
			assert_equal(self.process.send_request_get_response(test_request),
				self.process.response_factory.build(False, fake_id, ''))
			assert not verify_patch.called
			assert not pipe_patch.called
			assert not poll_patch.called

	def test_send_request_shutdown_not_shutdown(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='ghost_truck', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process.process, 'is_alive', return_value=True),
			mock.patch.object(self.process.pipe, 'close'),
			mock.patch.object(self.process.pipe, 'send_bytes'),
			mock.patch.object(self.process, '_poll_for_response', return_value=mock.Mock()),
			mock.patch.object(os, 'kill')
		) as (alive_patch, close_patch, send_patch, poll_patch, kill_patch):
			self.process.send_request_shutdown(test_request)
			alive_patch.assert_called_once_with()
			assert self.process.is_shutdown
			send_patch.assert_called_once_with(test_request.serialized)
			poll_patch.assert_called_once_with(fake_id, self.process.SHUTDOWN_TIMEOUT)
			close_patch.assert_called_once_with()
			kill_patch.assert_called_once_with(self.process.process.pid, signal.SIGKILL)

	def test_send_request_shutdown_is_shutdown(self):
		self.process.is_shutdown = True
		fake_id = 77
		test_request = mock.Mock(serialized='thats_million_bucks', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process.process, 'is_alive', return_value=True),
			mock.patch.object(self.process.pipe, 'close'),
			mock.patch.object(self.process.pipe, 'send_bytes'),
			mock.patch.object(self.process, '_poll_for_response', return_value=mock.Mock()),
			mock.patch.object(self.process.process, 'terminate')
		) as (alive_patch, close_patch, send_patch, poll_patch, terminate_patch):
			self.process.send_request_shutdown(test_request)
			assert not alive_patch.called  # should have short circuited
			close_patch.assert_called_once_with()
			assert self.process.is_shutdown
			assert not send_patch.called
			assert not poll_patch.called
			assert not terminate_patch.called

	def test_send_request_shutdown_not_shutdown_but_dead(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='thats_million_bucks', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process.process, 'is_alive', return_value=False),
			mock.patch.object(self.process.pipe, 'close'),
			mock.patch.object(self.process.pipe, 'send_bytes'),
			mock.patch.object(self.process, '_poll_for_response', return_value=mock.Mock()),
			mock.patch.object(self.process.process, 'terminate')
		) as (alive_patch, close_patch, send_patch, poll_patch, terminate_patch):
			self.process.send_request_shutdown(test_request)
			alive_patch.assert_called_once_with()
			close_patch.assert_called_once_with()
			assert self.process.is_shutdown
			assert not send_patch.called
			assert not poll_patch.called
			assert not terminate_patch.called

	def test_update_config(self):
		request = mock.Mock()
		new_config = mock.Mock()
		with mock.patch.object(self.process, 'send_request') as send_patch:
			self.process.update_config(new_config, request)
			send_patch.assert_called_once_with(request)
			assert_equal(self.process.config, new_config)

	def test_poll_for_response_has_response_makes_orphaned(self):
		self.process.orphaned_responses = {}
		fake_id = 77
		fake_timeout = 0.05
		fake_response_serial = ['first']
		fake_response_matching = mock.Mock(serialized=fake_response_serial, id=fake_id)
		fake_id_other = 96943
		fake_response_other = mock.Mock(serialized='oliver', id=fake_id_other)

		def recv_change_response():
			ret = fake_response_serial[0]
			fake_response_serial[0] = 'second'
			return ret

		def get_fake_response(fake_response_serial):
			if fake_response_serial == 'first':
				return fake_response_other
			else:
				return fake_response_matching

		with contextlib.nested(
			mock.patch.object(self.process.pipe, 'poll', return_value=True),
			mock.patch.object(self.process.pipe, 'recv_bytes', side_effect=recv_change_response),
			mock.patch.object(self.process.response_factory, 'rebuild', side_effect=get_fake_response)
		) as (poll_patch, recv_patch, rebuild_patch):
			assert_equal(self.process._poll_for_response(fake_id, fake_timeout), fake_response_matching)
			assert_equal(self.process.orphaned_responses, {fake_id_other: fake_response_other})
			poll_patch.assert_called_with(fake_timeout)
			assert_equal(poll_patch.call_count, 2)
			recv_patch.assert_called_with()
			assert_equal(recv_patch.call_count, 2)
			rebuild_patch.assert_called_with('second')
			assert_equal(rebuild_patch.call_count, 2)

	def test_poll_for_response_has_orphaned(self):
		fake_id = 77
		fake_timeout = 0.05
		fake_response = mock.Mock(serialized='wherein_there_is_dotes', id=fake_id)
		self.process.orphaned_responses = {fake_id: fake_response}
		with contextlib.nested(
			mock.patch.object(self.process.pipe, 'poll', return_value=True),
			mock.patch.object(self.process.pipe, 'recv_bytes'),
			mock.patch.object(self.process.response_factory, 'rebuild')
		) as (poll_patch, recv_patch, rebuild_patch):
			assert_equal(self.process._poll_for_response(fake_id, fake_timeout), fake_response)
			assert_equal(self.process.orphaned_responses, {})
			assert not poll_patch.called
			assert not recv_patch.called
			assert not rebuild_patch.called

	def test_poll_for_response_no_response(self):
		fake_id = 77
		fake_timeout = 0.05
		self.process.orphaned_responses = {}
		with contextlib.nested(
			mock.patch.object(self.process.pipe, 'poll', return_value=False),
			mock.patch.object(self.process.pipe, 'recv_bytes'),
			mock.patch.object(self.process.response_factory, 'rebuild')
		) as (poll_patch, recv_patch, rebuild_patch):
			assert_equal(self.process._poll_for_response(fake_id, fake_timeout), None)
			assert_equal(self.process.orphaned_responses, {})
			poll_patch.assert_called_once_with(fake_timeout)
			assert not recv_patch.called
			assert not rebuild_patch.called
