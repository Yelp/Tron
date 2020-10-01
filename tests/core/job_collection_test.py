import mock

from testifycompat import setup
from testifycompat import TestCase
from tests.testingutils import autospec_method
from tron.core.job import Job
from tron.core.job_collection import JobCollection
from tron.core.job_scheduler import JobScheduler
from tron.core.job_scheduler import JobSchedulerFactory


class TestJobCollection(TestCase):
    @setup
    def setup_collection(self):
        self.collection = JobCollection()

    def test_update_from_config(self):
        autospec_method(self.collection.jobs.filter_by_name)
        autospec_method(self.collection.add)
        factory = mock.create_autospec(JobSchedulerFactory)
        job_configs = {'a': mock.Mock(), 'b': mock.Mock()}
        result = self.collection.update_from_config(job_configs, factory, True)
        result = list(result)
        assert len(result) == len(job_configs)
        self.collection.jobs.filter_by_name.assert_called_with(job_configs)
        expected_calls = [mock.call(v) for v in job_configs.values()]
        assert factory.build.call_args_list == expected_calls
        assert self.collection.add.call_count == 2
        job_schedulers = [
            call[1][0] for call in self.collection.add.mock_calls[::2]
        ]
        for job_scheduler in job_schedulers:
            job_scheduler.schedule.assert_called_with()
            job_scheduler.get_job.assert_called_with()

    def test_update_from_config_reconfigure_one_namespace(self):
        autospec_method(self.collection.jobs.filter_by_name)
        autospec_method(self.collection.add)
        factory = mock.create_autospec(JobSchedulerFactory)
        job_configs = {'a.foo': mock.Mock(namespace='a'), 'b.foo': mock.Mock(namespace='b')}
        result = self.collection.update_from_config(job_configs, factory, True, namespace_to_reconfigure='a')
        result = list(result)
        assert len(result) == 1
        self.collection.jobs.filter_by_name.assert_called_with(job_configs)
        expected_calls = [mock.call(job_configs['a.foo'])]
        assert factory.build.call_args_list == expected_calls
        assert self.collection.add.call_count == 1
        job_schedulers = [
            call[1][0] for call in self.collection.add.mock_calls[::2]
        ]
        for job_scheduler in job_schedulers:
            job_scheduler.schedule.assert_called_with()
            job_scheduler.get_job.assert_called_with()

    def test_move_running_job(self):
        with mock.patch(
            'tron.core.job_collection.JobCollection.get_by_name',
            autospec=None
        ) as mock_scheduler:
            mock_scheduler.return_value.get_job.return_value.status = Job.STATUS_RUNNING
            result = self.collection.move('old.test', 'new.test')
            assert 'Job is still running.' in result

    def test_move(self):
        with mock.patch(
            'tron.core.job_collection.JobCollection.get_by_name',
            autospec=None
        ) as mock_scheduler:
            mock_scheduler.return_value.get_job.return_value.status = Job.STATUS_ENABLED
            mock_scheduler.get_name.return_value = 'old.test'
            self.collection.add(mock_scheduler)
            result = self.collection.move('old.test', 'new.test')
            assert 'succeeded' in result

    def test_update(self):
        mock_scheduler = mock.create_autospec(JobScheduler)
        existing_scheduler = mock.create_autospec(JobScheduler)
        autospec_method(
            self.collection.get_by_name,
            return_value=existing_scheduler,
        )
        assert self.collection.update(mock_scheduler)
        self.collection.get_by_name.assert_called_with(
            mock_scheduler.get_name(),
        )
        existing_scheduler.update_from_job_scheduler.assert_called_with(
            mock_scheduler
        )
        existing_scheduler.schedule_reconfigured.assert_called_with()
