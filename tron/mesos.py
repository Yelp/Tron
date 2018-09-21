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
    url = "%s:%s/redirect" % (master_address, mesos_master_port)
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


def get_secret_from_file(file_path):
    if file_path is not None:
        with open(file_path) as f:
            secret = f.read().strip()
    else:
        secret = None
    return secret


class MesosClusterRepository:
    """A class that stores MesosCluster objects and configuration."""

    # Config values
    mesos_enabled = False
    master_address = None
    master_port = None
    secret_file = None
    role = None
    principal = None
    default_volumes = ()
    dockercfg_location = None
    offer_timeout = None
    secret = None

    name = 'frameworks'
    clusters = {}
    state_data = {}
    state_watcher = None

    @classmethod
    def attach(cls, _, observer):
        cls.state_watcher = observer

    @classmethod
    def get_cluster(cls, master_address=None):
        if master_address is None:
            master_address = cls.master_address
        if master_address not in cls.clusters:
            framework_id = cls.state_data.get(master_address)
            cluster = MesosCluster(
                mesos_address=master_address,
                mesos_master_port=cls.master_port,
                secret=cls.secret,
                principal=cls.principal,
                mesos_role=cls.role,
                framework_id=framework_id,
                enabled=cls.mesos_enabled,
                default_volumes=cls.default_volumes,
                dockercfg_location=cls.dockercfg_location,
                offer_timeout=cls.offer_timeout,
            )
            cls.clusters[master_address] = cluster
        return cls.clusters[master_address]

    @classmethod
    def shutdown(cls):
        for cluster in cls.clusters.values():
            cluster.stop()

    @classmethod
    def configure(cls, mesos_options):
        cls.master_address = mesos_options.master_address
        cls.master_port = mesos_options.master_port
        cls.secret_file = mesos_options.secret_file
        cls.role = mesos_options.role
        cls.secret = get_secret_from_file(cls.secret_file)
        cls.principal = mesos_options.principal
        cls.mesos_enabled = mesos_options.enabled
        cls.default_volumes = [
            vol._asdict() for vol in mesos_options.default_volumes
        ]
        cls.dockercfg_location = mesos_options.dockercfg_location
        cls.offer_timeout = mesos_options.offer_timeout

        for cluster in cls.clusters.values():
            cluster.set_enabled(cls.mesos_enabled)
            cluster.configure_tasks(
                default_volumes=cls.default_volumes,
                dockercfg_location=cls.dockercfg_location,
                offer_timeout=cls.offer_timeout,
            )

    @classmethod
    def restore_state(cls, mesos_state):
        cls.state_data = mesos_state.get(cls.name, {})

    @classmethod
    def save(cls, master_address, framework_id):
        cls.state_data[master_address] = framework_id
        cls.state_watcher.handler(cls, None)

    @classmethod
    def remove(cls, master_address):
        if master_address in cls.state_data:
            del cls.state_data[master_address]
            cls.state_watcher.handler(cls, None)


class MesosTask(ActionCommand):
    ERROR_STATES = frozenset(['failed', 'killed', 'error'])

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
        # Every time a task gets created, this function runs and will add
        # more stderr handlers to the logger, which results in duplicate log
        # output. We only want to add the stderr handler if the logger does not
        # have a handler yet.
        if not len(log.handlers):
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
            self.log.warning(
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
            self.log.warning('Exception while logging event: {}'.format(e))

        if mesos_type == 'staging':
            pass
        elif mesos_type == 'running':
            self.started()
        elif mesos_type == 'finished':
            self.exited(0)
        elif mesos_type == 'lost':
            self.exited(None)
        elif mesos_type in self.ERROR_STATES:
            self.exited(1)
        else:
            self.log.warning(
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
    def __init__(
        self,
        mesos_address,
        mesos_master_port=None,
        secret=None,
        principal=None,
        mesos_role=None,
        framework_id=None,
        enabled=True,
        default_volumes=None,
        dockercfg_location=None,
        offer_timeout=None,
    ):
        self.mesos_address = mesos_address
        self.mesos_master_port = mesos_master_port
        self.secret = secret
        self.principal = principal
        self.mesos_role = mesos_role
        self.enabled = enabled
        self.default_volumes = default_volumes or []
        self.dockercfg_location = dockercfg_location
        self.offer_timeout = offer_timeout
        self.framework_id = framework_id

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

    def configure_tasks(
        self,
        default_volumes,
        dockercfg_location,
        offer_timeout,
    ):
        self.default_volumes = default_volumes
        self.dockercfg_location = dockercfg_location
        self.offer_timeout = offer_timeout

    def connect(self):
        self.runner = self.get_runner(self.mesos_address, self.queue)
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

    def _check_connection(self):
        if self.runner.stopping:
            # Last framework was terminated for some reason, re-connect.
            log.info('Last framework stopped, re-connecting')
            self.connect()
        elif self.deferred.called:
            # Just in case callbacks are missing, re-add.
            self.handle_next_event()

    def submit(self, task, recover=False):
        if not task:
            return

        if not self.enabled:
            task.log.info('Task failed to start, Mesos is disabled.')
            task.exited(1)
            return
        self._check_connection()

        mesos_task_id = task.get_mesos_id()
        self.tasks[mesos_task_id] = task
        self.runner.run(task.get_config())
        log.info(
            'Submitting task {} to {}'.format(
                mesos_task_id,
                self.mesos_address,
            ),
        )

    def recover(self, task):
        if not task:
            return

        if not self.enabled:
            task.log.info('Could not recover task, Mesos is disabled.')
            task.exited(None)
            return
        self._check_connection()

        mesos_task_id = task.get_mesos_id()
        self.tasks[mesos_task_id] = task
        task.log.info('Reconciling state for this task from Mesos')
        task.started()
        self.runner.reconcile(task.get_config())

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
        task_id=None,
    ):
        if not self.runner:
            return None

        uris = [self.dockercfg_location] if self.dockercfg_location else []
        volumes = combine_volumes(self.default_volumes, extra_volumes)
        task_kwargs = {
            'name': action_run_id,
            'cmd': command,
            'cpus': cpus,
            'mem': mem,
            'constraints': constraints,
            'image': docker_image,
            'docker_parameters': docker_parameters,
            'environment': env,
            'volumes': volumes,
            'uris': uris,
            'offer_timeout': self.offer_timeout,
        }
        task_config = self.runner.TASK_CONFIG_INTERFACE(**task_kwargs)

        if task_id is not None:
            try:
                task_config = task_config.set_task_id(task_id)
            except ValueError:
                log.error(f'Invalid {task_id} for {action_run_id}')
                return

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
            provider='mesos_task',
            provider_config={
                'secret':
                    self.secret,
                'principal':
                    self.principal,
                'mesos_address':
                    get_mesos_leader(mesos_address, self.mesos_master_port),
                'role':
                    self.mesos_role,
                'framework_name':
                    framework_name,
                'framework_id':
                    self.framework_id,
                'failover':
                    True,
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
                log.warning('Framework has been stopped: {}'.format(event.raw))
                self.stop()
                MesosClusterRepository.remove(self.mesos_address)
            elif message == 'unknown':
                log.warning(
                    'Unknown error from Mesos master: {}'.format(event.raw)
                )
            elif message == 'registered':
                framework_id = event.raw['framework_id']['value']
                MesosClusterRepository.save(self.mesos_address, framework_id)
            else:
                log.warning('Unknown type of control event: {}'.format(event))

        elif event.kind == 'task':
            if not hasattr(event, 'task_id'):
                log.warning('Task event missing task_id: {}'.format(event))
                return
            if event.task_id not in self.tasks:
                log.warning(
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
            log.warning('Unknown type of event: {}'.format(event))

    def stop(self):
        self.framework_id = None
        if self.runner:
            self.runner.stop()

        # Clear message queue
        if self.deferred:
            self.deferred.cancel()
        self.queue = PyDeferredQueue()

        for key, task in list(self.tasks.items()):
            task.exited(None)
            del self.tasks[key]

    def kill(self, task_id):
        return self.runner.kill(task_id)
