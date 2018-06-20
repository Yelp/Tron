import logging
import socket
from urllib.parse import urlparse

import requests
from task_processing.runners.subscription import Subscription
from task_processing.task_processor import TaskProcessor
from twisted.internet.defer import logError

from tron.actioncommand import ActionCommand
from tron.utils.dicts import get_deep
from tron.utils.queue import PyDeferredQueue

TASK_LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s %(message)s'
TASK_OUTPUT_LOGGER = 'tron.mesos.task_output'

# TODO: put in configs
MESOS_MASTER_PORT = 5050
DOCKERCFG_LOCATION = "file:///root/.dockercfg"
MESOS_SECRET = ''
MESOS_ROLE = '*'
OFFER_TIMEOUT = 300

log = logging.getLogger(__name__)


def get_mesos_leader(master_address):
    url = "http://%s:%s/redirect" % (master_address, MESOS_MASTER_PORT)
    response = requests.get(url)
    return '{}:{}'.format(urlparse(response.url).hostname, MESOS_MASTER_PORT)


class MesosClusterRepository:
    """A class that stores MesosCluster objects and configuration."""

    clusters = {}
    mesos_enabled = False

    @classmethod
    def get_cluster(cls, master_address):
        if master_address not in cls.clusters:
            cls.clusters[master_address] = MesosCluster(
                master_address, cls.mesos_enabled
            )
        return cls.clusters[master_address]

    @classmethod
    def shutdown(cls):
        for cluster in cls.clusters.values():
            cluster.stop()

    @classmethod
    def configure(cls, mesos_options):
        mesos_enabled = mesos_options.enabled
        cls.mesos_enabled = mesos_enabled
        for cluster in cls.clusters.values():
            cluster.set_enabled(mesos_enabled)


class MesosTask(ActionCommand):
    ERROR_STATES = frozenset(['failed', 'killed', 'lost', 'error'])

    def __init__(self, id, task_config, serializer=None):
        super(MesosTask, self).__init__(id, task_config.cmd, serializer)
        self.task_config = task_config
        self.log = self.get_event_logger()
        self.setup_output_logging()

        self.log.info(
            'Mesos task {} created with config {}'.format(
                self.get_mesos_id(),
                self.get_config(),
            ),
        )

    def get_event_logger(self):
        log = logging.getLogger(__name__ + '.' + self.id)
        handler = logging.StreamHandler(self.stderr)
        handler.setFormatter(logging.Formatter(TASK_LOG_FORMAT))
        log.addHandler(handler)
        return log

    def setup_output_logging(self):
        task_id = self.get_mesos_id()
        stdout_logger = logging.getLogger(
            '{}.{}.{}'.format(TASK_OUTPUT_LOGGER, task_id, 'stdout'),
        )
        stdout_logger.addHandler(logging.StreamHandler(self.stdout))
        stderr_logger = logging.getLogger(
            '{}.{}.{}'.format(TASK_OUTPUT_LOGGER, task_id, 'stderr'),
        )
        stderr_logger.addHandler(logging.StreamHandler(self.stderr))

    def get_mesos_id(self):
        return self.task_config.task_id

    def get_config(self):
        return self.task_config

    def log_event_info(self, event):
        # Separate out so task still transitions even if this nice-to-have logging fails.
        mesos_type = getattr(event, 'platform_type', None)
        if mesos_type == 'staging':
            # TODO: Save these in state?
            agent = get_deep(event.raw, 'offer.agent_id.value')
            hostname = get_deep(event.raw, 'offer.hostname')
            self.log.info(
                'Staging task on agent {agent} (hostname {hostname})'.format(
                    agent=agent,
                    hostname=hostname,
                ),
            )
        elif mesos_type == 'running':
            agent = get_deep(event.raw, 'agent_id.value')
            self.log.info('Running on agent {agent}'.format(agent=agent))
        elif mesos_type == 'finished':
            pass
        elif mesos_type in self.ERROR_STATES:
            self.log.error('Error from Mesos: {}'.format(event.raw))

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
        try:
            self.log_event_info(event)
        except Exception as e:
            self.log.warn('Exception while logging event: {}'.format(e))

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

            exit_code = int(not getattr(event, 'success', False))
            # Returns False if we've already exited normally above
            unexpected_error = self.exited(exit_code)
            if unexpected_error:
                self.log.error('Unknown failure, exiting')

            self.done()


class MesosCluster:
    def __init__(self, mesos_address, enabled=True):
        self.mesos_address = mesos_address
        self.enabled = enabled
        self.processor = TaskProcessor()
        self.queue = PyDeferredQueue()
        self.deferred = None
        self.runner = None
        self.tasks = {}

        self.processor.load_plugin(
            provider_module='task_processing.plugins.mesos'
        )
        self.connect()

    def set_enabled(self, is_enabled):
        self.enabled = is_enabled
        if is_enabled:
            self.connect()
        else:
            self.stop()

    # TODO: Should this be done asynchronously?
    # TODO: Handle/retry errors
    def connect(self):
        self.runner = self.get_runner(self.mesos_address, self.queue)
        self.handle_next_event()

    def handle_next_event(self, deferred_result=None):
        if self.deferred and not self.deferred.called:
            log.warn(
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
        if not task:
            return

        if not self.enabled:
            task.log.info('Task failed to start, Mesos is disabled.')
            task.exited(1)
            return

        if self.runner.stopping:
            # Last framework was terminated for some reason, re-connect.
            self.connect()
        elif self.deferred.called:
            # Just in case callbacks are missing, re-add.
            self.handle_next_event()

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
        if not self.runner:
            return None

        task_config = self.runner.TASK_CONFIG_INTERFACE(
            name=action_run_id,
            cmd=command,
            cpus=cpus,
            mem=mem,
            constraints=constraints,
            image=docker_image,
            docker_parameters=docker_parameters,
            environment=env,
            volumes=extra_volumes,  # TODO: add default volumes
            uris=[DOCKERCFG_LOCATION],
            offer_timeout=OFFER_TIMEOUT,
        )
        return MesosTask(action_run_id, task_config, serializer)

    def get_runner(self, mesos_address, queue):
        if not self.enabled:
            log.info('Mesos is disabled, not creating a framework.')
            return None

        if self.runner and not self.runner.stopping:
            log.info('Already have a running framework, not creating one.')
            return self.runner

        framework_name = 'tron-{}'.format(socket.gethostname())
        executor = self.processor.executor_from_config(
            provider='mesos',
            provider_config={
                'secret': MESOS_SECRET,
                'mesos_address': get_mesos_leader(mesos_address),
                'role': MESOS_ROLE,
                'framework_name': framework_name,
            }
        )

        def log_output(task_id, message, stream):
            logger = logging.getLogger(
                '{}.{}.{}'.format(
                    TASK_OUTPUT_LOGGER,
                    task_id,
                    stream,
                )
            )
            logger.info(message)

        logging_executor = self.processor.executor_from_config(
            provider='logging',
            provider_config={
                'downstream_executor': executor,
                'handler': log_output,
                'format_string': '{line}',
            },
        )
        return Subscription(logging_executor, queue)

    def _process_event(self, event):
        if event.kind == 'control':
            message = getattr(event, 'message', None)
            if message == 'stop':
                # Framework has been removed, stop it.
                log.warn('Framework has been stopped: {}'.format(event.raw))
                self.stop()
            elif message == 'unknown':
                log.warn(
                    'Unknown error from Mesos master: {}'.format(event.raw)
                )
            else:
                log.warn('Unknown type of control event: {}'.format(event))

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
        if self.runner:
            self.runner.stop()
        if self.deferred:
            self.deferred.cancel()
        for key, task in list(self.tasks.items()):
            task.log.warning(
                'Still running during Mesos shutdown, becoming unknown'
            )
            task.exited(None)
            del self.tasks[key]
