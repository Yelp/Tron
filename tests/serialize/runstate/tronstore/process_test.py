import contextlib
import mock
from testify import TestCase, run, assert_equal, assert_raises, setup_teardown
from tron.serialize.runstate.tronstore.process import StoreProcessProtocol, TronStoreError
from tron.serialize.runstate.tronstore import chunking

class StoreProcessProtocolTestCase(TestCase):

	@setup_teardown
	def setup_process(self):
		with mock.patch('twisted.internet.reactor.stop', autospec=True) as self.stop_patch:
			self.factory = mock.Mock()
			self.process = StoreProcessProtocol(self.factory)
			yield

	def test__init__(self):
		assert_equal(self.process.response_factory, self.factory)
		assert_equal(self.process.requests, {})
		assert_equal(self.process.responses, {})
		assert_equal(self.process.semaphores, {})
		assert not self.process.is_shutdown

	def test_outRecieved_chunked(self):
		data = 'deadly_premonition_br'
		self.process.outRecieved(data)
		assert_equal(self.process.chunker.chunk, data)

	def test_outRecieved_full_no_monitor(self):
		data = 'no_one_str_should_have_all_that_power' + chunking.CHUNK_SIGNING_STR
		fake_id = 77
		fake_response = mock.Mock(id=fake_id, success=True)
		self.factory.rebuild = mock.Mock(return_value=fake_response)
		self.process.requests[fake_id] = fake_response
		self.process.outRecieved(data)
		assert_equal(self.process.chunker.chunk, '')
		assert_equal(self.process.responses, {})
		assert_equal(self.process.requests, {})
		assert_equal(self.process.semaphores, {})

	def test_outRecieved_full_with_monitor(self):
		data = 'throwing_dark' + chunking.CHUNK_SIGNING_STR
		fake_id = 77
		fake_response = mock.Mock(id=fake_id, success=True)
		self.factory.rebuild = mock.Mock(return_value=fake_response)
		fake_semaphore = mock.Mock()
		self.process.semaphores[fake_id] = fake_semaphore
		self.process.requests[fake_id] = fake_response
		self.process.outRecieved(data)
		assert_equal(self.process.chunker.chunk, '')
		assert_equal(self.process.responses, {fake_id: fake_response})
		assert_equal(self.process.semaphores, {fake_id: fake_semaphore})
		assert_equal(self.process.requests, {})
		fake_semaphore.release.assert_called_once_with()

	def test_processExited_running(self):
		self.process.is_shutdown = False
		assert_raises(TronStoreError, self.process.processExited, mock.Mock(getErrorMessage=lambda: 'test'))

	def test_processExited_shutdown(self):
		self.process.is_shutdown = True
		self.process.processExited('its_a_website')

	def test_processEnded_running(self):
		self.process.is_shutdown = False
		with mock.patch.object(self.process, 'transport') as lose_patch:
			self.process.processEnded('about_videogames')
			lose_patch.loseConnection.assert_called_once_with()
			self.stop_patch.assert_called_once_with()

	def test_processEnded_shutdown(self):
		self.process.is_shutdown = True
		with mock.patch.object(self.process, 'transport') as lose_patch:
			self.process.processEnded('this_aint_no_game')
			assert not self.stop_patch.called
			assert not lose_patch.loseConnection.called

	def test_send_request_running(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='sunny_sausalito', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process, 'transport'),
			mock.patch.object(self.process.chunker, 'sign')
		) as (trans_patch, sign_patch):
			self.process.send_request(test_request)
			assert_equal(self.process.requests[fake_id], test_request)
			sign_patch.assert_called_once_with(test_request.serialized)
			trans_patch.write.assert_called_once_with(self.process.chunker.sign(test_request.serialized))

	def test_send_request_shutdown(self):
		self.process.is_shutdown = True
		fake_id = 77
		test_request = mock.Mock(serialized='whiskey_media', id=fake_id)
		with contextlib.nested(
			mock.patch.object(self.process, 'transport'),
			mock.patch.object(self.process.chunker, 'sign')
		) as (trans_patch, sign_patch):
			self.process.send_request(test_request)
			assert_equal(self.process.requests, {})
			assert not sign_patch.called
			assert not trans_patch.write.called

	def test_send_request_get_response_running(self):
		self.process.is_shutdown = False
		fake_id = 77
		test_request = mock.Mock(serialized='objection', id=fake_id)
		test_response = mock.Mock(id=fake_id, data='overruled', success=True)
		self.process.responses[fake_id] = test_response
		with contextlib.nested(
			mock.patch.object(self.process, 'transport'),
			mock.patch.object(self.process.chunker, 'sign'),
			mock.patch('tron.serialize.runstate.tronstore.process.Semaphore', autospec=True)
		) as (trans_patch, sign_patch, sema_patch):
			assert_equal(self.process.send_request_get_response(test_request), test_response.data)
			assert_equal(self.process.requests, {fake_id: test_request})
			assert_equal(self.process.semaphores, {})
			assert_equal(self.process.responses, {})
			sign_patch.assert_called_once_with(test_request.serialized)
			trans_patch.write.assert_called_once_with(self.process.chunker.sign(test_request.serialized))
			sema_patch.assert_called_once_with(0)

	def test_send_request_get_response_shutdown(self):
		self.process.is_shutdown = True
		fake_id = 77
		test_request = mock.Mock(serialized='does_he_look_like_a', id=fake_id)
		test_response = mock.Mock(id=fake_id, data='what', success=True)
		self.process.responses[fake_id] = test_response
		with contextlib.nested(
			mock.patch.object(self.process, 'transport'),
			mock.patch.object(self.process.chunker, 'sign'),
			mock.patch('tron.serialize.runstate.tronstore.process.Semaphore', autospec=True)
		) as (trans_patch, sign_patch, sema_patch):
			assert_equal(self.process.send_request_get_response(test_request), None)
			assert_equal(self.process.requests, {})
			assert_equal(self.process.semaphores, {})
			assert_equal(self.process.responses, {fake_id: test_response})
			assert not sign_patch.called
			assert not trans_patch.write.called
			assert not sema_patch.called

	def test_shutdown(self):
		self.process.is_shutdown = False
		with mock.patch.object(self.process, 'transport') as trans_patch:
			self.process.shutdown()
			assert self.process.is_shutdown
			trans_patch.signalProcess.assert_called_once_with('INT')
			trans_patch.loseConnection.assert_called_once_with()
			self.stop_patch.assert_called_once_with()
