import json
import logging
from urllib.parse import urlparse

import requests
from task_processing.runners.subscription import Subscription
from task_processing.task_processor import TaskProcessor
from twisted.internet.defer import DeferredQueue

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
    def __init__(self, id, task_config, serializer=None):
        super(MesosTask, self).__init__(id, task_config.cmd, serializer)
        self.task_config = task_config
        self.mesos_task_id = None
        self.log = self.setup_logger()

    def setup_logger(self):
        log = logging.getLogger(__name__ + '.' + self.id)
        handler = logging.StreamHandler(self.stderr)
        handler.setFormatter(logging.Formatter(TASK_LOG_FORMAT))
        log.addHandler(handler)
        return log


class MesosCluster:

    # TODO: does it create a connection on init? should it be async?
    def __init__(self, mesos_address):
        self.processor = get_mesos_processor()
        self.queue = DeferredQueue()
        self.deferred = self.queue.get()
        self.deferred.addCallback(self.handle_event)
        # TODO: addErrback?
        self.runner = self.get_runner(mesos_address, self.queue)
        self.tasks = {}

    def submit(self, task):
        self.tasks[task.id] = task
        self.runner.run(task.task_config)

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

    def handle_event(self, event):
        log.info(
            'Task {id}, {type}'.format(
                id=event.task_id, type=event.platform_type
            )
        )
        action_run_id = event.task_config.name
        if action_run_id not in self.tasks:
            log.warning(
                'Got event for unknown action run: {}'.format(action_run_id)
            )
        else:
            task = self.tasks[action_run_id]
            task.write_stdout(json.dumps(event.raw))
            if event.platform_type == 'staging':
                task.mesos_task_id = event.task_id
                # TODO: save task_id in action run state
            elif event.platform_type == 'running':
                task.started()
            elif event.platform_type == 'finished':
                task.exited(0)
            elif event.platform_type == 'failed':
                task.exited(1)
                log.error('Task failed')  # todo
            elif event.platform_type == 'killed':
                task.exited(1)
                log.info('Task killed')
            elif event.platform_type == 'lost':
                task.exited(1)
                log.info('Task lost, should retry')
            elif event.platform_type == 'error':
                task.exited(1)
                log.info('Task error, {}'.format(event.raw))

            if event.terminal:
                task.done()
                del self.tasks[action_run_id]

        self.deferred = self.queue.get()
        self.deferred.addCallback(self.handle_event)

    def stop(self):
        self.runner.stop()
        self.deferred.cancel()
