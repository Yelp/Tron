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

log = logging.getLogger(__name__)


def get_mesos_leader(master_address, mesos_master_port):
    url = "http://%s:%s/redirect" % (master_address, mesos_master_port)
    response = requests.get(url)
    return '{}:{}'.format(urlparse(response.url).hostname, mesos_master_port)


def combine_volumes(defaults, overrides):
    """Helper to reconcile lists of volume mounts.

    If any volumes have the same container path, the one in overrides wins.
    """
    result = {mount['container_path']: mount for mount in defaults}
    for mount in overrides:
        result[mount['container_path']] = mount
    return list(result.values())


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
    mesos_master_address = None,
    mesos_master_port = None,
    mesos_secret = None,
    mesos_role = None,
    mesos_enabled = False
    default_volumes = ()
    dockercfg_location = None
    offer_timeout = None
    framework_id = None

    processor = TaskProcessor()
    queue = PyDeferredQueue()
    deferred = None
    runner = None
    tasks = {}

    processor.load_plugin(
        provider_module='task_processing.plugins.mesos'
    )

    name = 'frameworks'
    state_data = {}
    state_watcher = None

    @classmethod
    def attach(cls, _, observer):
        cls.state_watcher = observer

    @classmethod
    def configure(cls, mesos_options):
        cls.mesos_master_address = mesos_options.master_address
        cls.mesos_master_port = mesos_options.master_port
        cls.mesos_secret = mesos_options.secret
        cls.mesos_role = mesos_options.role
        cls.mesos_enabled = mesos_options.enabled
        cls.default_volumes = [
            vol._asdict() for vol in mesos_options.default_volumes
        ]
        cls.dockercfg_location = mesos_options.dockercfg_location
        cls.offer_timeout = mesos_options.offer_timeout

    # TODO: Should this be done asynchronously?
    # TODO: Handle/retry errors
    @classmethod
    def connect(cls):
        cls.runner = cls.get_runner(cls.queue)
        cls.handle_next_event()

    @classmethod
    def handle_next_event(cls, deferred_result=None):
        if cls.deferred and not cls.deferred.called:
            log.warn(
                'Already have handlers waiting for next event in queue, '
                'not adding more'
            )
            return
        cls.deferred = cls.queue.get()
        cls.deferred.addCallback(cls._process_event)
        cls.deferred.addCallback(cls.handle_next_event)
        cls.deferred.addErrback(logError)
        cls.deferred.addErrback(cls.handle_next_event)

    @classmethod
    def submit(cls, task):
        if not task:
            return

        if not cls.mesos_enabled:
            task.log.info('Task failed to start, Mesos is disabled.')
            task.exited(1)
            return

        if cls.runner is None or cls.runner.stopping:
            # Last framework was terminated for some reason, re-connect.
            cls.connect()
        elif cls.deferred.called:
            # Just in case callbacks are missing, re-add.
            cls.handle_next_event()

        mesos_task_id = task.get_mesos_id()
        cls.tasks[mesos_task_id] = task
        cls.runner.run(task.get_config())
        log.info(
            'Submitting task {} to {}'.format(
                mesos_task_id,
                cls.mesos_master_address,
            ),
        )

    @classmethod
    def create_task(
        cls,
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
        if not cls.runner:
            return None

        uris = [cls.dockercfg_location] if cls.dockercfg_location else []
        volumes = combine_volumes(cls.default_volumes, extra_volumes)
        task_config = cls.runner.TASK_CONFIG_INTERFACE(
            name=action_run_id,
            cmd=command,
            cpus=cpus,
            mem=mem,
            constraints=constraints,
            image=docker_image,
            docker_parameters=docker_parameters,
            environment=env,
            volumes=volumes,
            uris=uris,
            offer_timeout=cls.offer_timeout,
        )
        return MesosTask(action_run_id, task_config, serializer)

    @classmethod
    def get_runner(cls, queue):
        if not cls.mesos_enabled:
            log.info('Mesos is disabled, not creating a framework.')
            return None

        if cls.runner and not cls.runner.stopping:
            log.info('Already have a running framework, not creating one.')
            return cls.runner

        framework_name = 'tron-{}'.format(socket.gethostname())
        print("At get_runner, framework_id = {}".format(cls.framework_id))

        executor = cls.processor.executor_from_config(
            provider='mesos_task',
            provider_config={
                'secret': cls.mesos_secret,
                'mesos_address': get_mesos_leader(cls.mesos_master_address, cls.mesos_master_port),
                'role': cls.mesos_role,
                'framework_name': framework_name,
                'framework_id': cls.framework_id,
                'failover': True,
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

        logging_executor = cls.processor.executor_from_config(
            provider='logging',
            provider_config={
                'downstream_executor': executor,
                'handler': log_output,
                'format_string': '{line}',
            },
        )
        return Subscription(logging_executor, queue)

    @classmethod
    def _process_event(cls, event):
        if event.kind == 'control':
            message = getattr(event, 'message', None)
            if message == 'stop':
                # Framework has been removed, stop it.
                log.warn('Framework has been stopped: {}'.format(event.raw))
                cls.stop()
            elif message == 'unknown':
                log.warn(
                    'Unknown error from Mesos master: {}'.format(event.raw)
                )
            elif message == 'registered':
                framework_id = event.raw['framework_id']['value']
                cls.save(framework_id)
            else:
                log.warn('Unknown type of control event: {}'.format(event))

        elif event.kind == 'task':
            if not hasattr(event, 'task_id'):
                log.warn('Task event missing task_id: {}'.format(event))
                return
            if event.task_id not in cls.tasks:
                log.warn(
                    'Received event for unknown task {}: {}'.format(
                        event.task_id,
                        event,
                    ),
                )
                return
            task = cls.tasks[event.task_id]
            task.handle_event(event)
            if task.is_done:
                del cls.tasks[event.task_id]
        else:
            log.warn('Unknown type of event: {}'.format(event))

    @classmethod
    def stop(cls):
        cls.framework_id = None
        if cls.runner:
            cls.runner.stop()
        if cls.deferred:
            cls.deferred.cancel()
        for key, task in list(cls.tasks.items()):
            task.log.warning(
                'Still running during Mesos shutdown, becoming unknown'
            )
            task.exited(None)
            del cls.tasks[key]

    @classmethod
    def kill(cls, task_id):
        return cls.runner.kill(task_id)

    @classmethod
    def restore_state(cls, mesos_state):
        cls.state_data = mesos_state.get(cls.name, {})
        if cls.mesos_enabled:
            try:
                cls.framework_id = cls.state_data.get(cls.mesos_master_address)
            except AttributeError:
                log.warn('Can not retrieve framework_id from state file. Tron would create a new framework')
            cls.connect()

    @classmethod
    def save(cls, framework_id):
        cls.state_data[cls.mesos_master_address] = framework_id
        cls.state_watcher.handler(cls, None)
