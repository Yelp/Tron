import json
import logging
from urllib.parse import urlparse

import requests
from task_processing.runners.subscription import Subscription
from task_processing.task_processor import TaskProcessor
from twisted.internet.defer import DeferredQueue
from twisted.internet.defer import logError

from tron.actioncommand import ActionCommand

TASK_LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s %(message)s'
# TODO: put in configs
MESOS_MASTER_PORT = 5050
DOCKERCFG_LOCATION = "file:///root/.dockercfg"
MESOS_SECRET = ''
MESOS_ROLE = '*'

log = logging.getLogger(__name__)
_processor = None
frameworks = {}  # TODO: improve


def get_mesos_processor():
    global _processor
    if not _processor:
        _processor = TaskProcessor()
        _processor.load_plugin(provider_module='task_processing.plugins.mesos')
    return _processor


def get_mesos_leader(master_address):
    url = "http://%s:%s/redirect" % (master_address, MESOS_MASTER_PORT)
    response = requests.get(url)
    return '{}:{}'.format(urlparse(response.url).hostname, MESOS_MASTER_PORT)


def get_mesos_cluster(master_address):
    global frameworks
    if master_address not in frameworks:
        frameworks[master_address] = MesosCluster(master_address)
    return frameworks[master_address]


def shutdown_frameworks():
    global frameworks
    for name, framework in frameworks.items():
        framework.stop()


class MesosTask(ActionCommand):
    ERROR_STATES = frozenset(['failed', 'killed', 'lost', 'error'])

    def __init__(self, id, task_config, serializer=None):
        super(MesosTask, self).__init__(id, task_config.cmd, serializer)
        self.task_config = task_config
        self.log = self.setup_logger()

        self.log.info(
            'Mesos task {} created with config {}'.format(
                self.get_mesos_id(),
                self.get_config(),
            ),
        )

    def setup_logger(self):
        log = logging.getLogger(__name__ + '.' + self.id)
        handler = logging.StreamHandler(self.stderr)
        handler.setFormatter(logging.Formatter(TASK_LOG_FORMAT))
        log.addHandler(handler)
        return log

    def get_mesos_id(self):
        return self.task_config.task_id

    def get_config(self):
        return self.task_config

    def handle_event(self, event):
        event_id = getattr(event, 'task_id', None)
        if event_id != self.get_mesos_id():
            self.log.warn(
                'Event task id {} does not match, ignoring'.format(event_id),
            )
            return
        mesos_type = getattr(event, 'platform_type', None)
        self.log.info(
            'Got event for task {id}, Mesos type {type}'.format(
                id=event_id,
                type=mesos_type,
            )
        )

        if mesos_type == 'staging':
            pass
        elif mesos_type == 'running':
            self.started()
        elif mesos_type == 'finished':
            self.exited(0)
        elif mesos_type in self.ERROR_STATES:
            self.exited(1)
        else:
            self.log.warn(
                'Did not handle unknown type of event: {}'.format(event),
            )

        if event.terminal:
            self.log.info('Event was terminal, closing task')
            # Returns False if we've already exited normally above
            unexpected_error = self.exited(None)
            if unexpected_error:
                self.log.error('Unknown failure, exiting')
            self.done()


class MesosCluster:

    # TODO: does it create a connection on init? should it be async?
    def __init__(self, mesos_address):
        self.mesos_address = mesos_address
        self.processor = get_mesos_processor()
        self.queue = DeferredQueue()
        self.deferred = None
        self.runner = self.get_runner(mesos_address, self.queue)
        self.tasks = {}
        self.handle_next_event()

    def handle_next_event(self, deferred_result=None):
        if self.deferred and not self.deferred.called:
            log.warning(
                'Already have handlers waiting for next event in queue, '
                'not adding more'
            )
            return
        self.deferred = self.queue.get()
        self.deferred.addCallback(self._process_event)
        self.deferred.addCallback(self.handle_next_event)
        self.deferred.addErrback(logError)
        self.deferred.addErrback(self.handle_next_event)

    def submit(self, task):
        mesos_task_id = task.get_mesos_id()
        self.tasks[mesos_task_id] = task
        self.runner.run(task.get_config())
        log.info(
            'Submitting task {} to {}'.format(
                mesos_task_id,
                self.mesos_address,
            ),
        )

    def create_task(
        self,
        action_run_id,
        command,
        cpus,
        mem,
        constraints,
        docker_image,
        docker_parameters,
        env,
        extra_volumes,
        serializer,
    ):
        task_config = self.runner.TASK_CONFIG_INTERFACE(
            name=action_run_id,
            cmd=command,
            cpus=cpus,
            mem=mem,
            constraints=constraints,  # TODO: format back to dict
            image=docker_image,
            # TODO: format. and should ulimit, cap_add be passed in directly?
            docker_parameters=docker_parameters,
            environment=env,
            volumes=extra_volumes,  # TODO: add default volumes
            uris=[DOCKERCFG_LOCATION],
        )
        return MesosTask(action_run_id, task_config, serializer)

    def get_runner(self, mesos_address, queue):
        executor = self.processor.executor_from_config(
            provider='mesos',
            provider_config={
                'secret': MESOS_SECRET,
                'mesos_address': get_mesos_leader(mesos_address),
                'role': MESOS_ROLE,
                # TODO: could also be in config, to include Tron cluster name
                'framework_name': 'tron',
            }
        )
        return Subscription(executor, queue)

    def _process_event(self, event):
        if event.kind == 'control':
            log.info('Control event: {}'.format(event))
        elif event.kind == 'task':
            if not hasattr(event, 'task_id'):
                log.warn('Task event missing task_id: {}'.format(event))
                return
            if event.task_id not in self.tasks:
                log.warn(
                    'Received event for unknown task {}: {}'.format(
                        event.task_id,
                        event,
                    ),
                )
                return
            task = self.tasks[event.task_id]
            task.handle_event(event)
            if task.is_done:
                del self.tasks[event.task_id]
        else:
            log.warn('Unknown type of event: {}'.format(event))

    def stop(self):
        self.runner.stop()
        self.deferred.cancel()
