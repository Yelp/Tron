from testify import *

from tron import event

class SimpleStoreTestCase(TestCase):
	@setup
	def build_store(self):
		self.store = event.FixedLimitStore({event.LEVEL_INFO: 2})
	
	@setup
	def add_data(self):
		self.store.append("test1", event.LEVEL_INFO)
		self.store.append("test2", event.LEVEL_INFO)
		self.store.append("test3", event.LEVEL_INFO)
		self.store.append("test4", event.LEVEL_INFO)

		self.store.append("test5", event.LEVEL_CRITICAL)
		self.store.append("test6", event.LEVEL_CRITICAL)
		self.store.append("test7", event.LEVEL_CRITICAL)
		self.store.append("test8", event.LEVEL_CRITICAL)
		self.store.append("test9", event.LEVEL_CRITICAL)
	def test(self):
		values = list(self.store)
		
		assert_not_in("test1", values)
		assert_not_in("test2", values)
		assert_in("test3", values)
		assert_in("test4", values)

		assert_in("test5", values)
		assert_in("test6", values)
		assert_in("test7", values)
		assert_in("test8", values)
		assert_in("test9", values)


class ParentEventRecorderTestCase(TestCase):
	@setup
	def build_recorders(self):
		self.parent_recorder = event.EventRecorder(self)
		self.recorder = event.EventRecorder(self, parent=self.parent_recorder)
	
	def test(self):
		self.recorder.record(event.Event(self, event.LEVEL_INFO, "hello"))
		self.recorder.emit_notice("hello again")
		
		assert_equal(len(self.recorder.list()), 2)
		assert_equal(len(self.parent_recorder.list()), 1)

		assert_equal(len(self.recorder.list(min_level=event.LEVEL_CRITICAL)), 0)
		assert_equal(len(self.recorder.list(min_level=event.LEVEL_NOTICE)), 1)


class EntitySwapTestCase(TestCase):
	"""Our recorder should be able to swap out the underlying entity, and all the associated events should be updated"""
	@setup
	def build_recorder(self):
		self.entity = turtle.Turtle()
		self.recorder = event.EventRecorder(self)
	
	@setup
	def create_event(self):
		self.recorder.emit_notice("hello")
	
	def test(self):
		self.recorder.entity = self.entity
		
		evt = self.recorder.list()[0]
		assert_equal(evt.entity, self.entity)