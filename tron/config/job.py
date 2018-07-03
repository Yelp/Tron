import datetime

import pytz
from pyrsistent import CheckedPMap
from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap

from tron.config import config_utils
from tron.config import ConfigRecord
from tron.config import schedule_parse
from tron.config.config_utils import IDENTIFIER_RE
from tron.config.schema import CLEANUP_ACTION_NAME
from tron.core.action import Action
from tron.core.action import ActionMap


def inv_identifier(v):
    return (
        bool(IDENTIFIER_RE.match(v)), "{} is not a valid identifier".format(v)
    )


def inv_name_identifier(name: str):
    name_parts = name.split('.', 1)
    if len(name_parts) == 1:
        name = name_parts[0]
    else:
        name = name_parts[1]

    return inv_identifier(name)


def inv_actions(actions):
    if len(actions) == 0:
        return (False, "`actions` can't be empty")

    for an, av in actions.items():
        for dep in av.requires:
            if dep not in actions:
                return (
                    False,
                    "`actions` contains external dependency: {} -> {}".format(
                        an, dep
                    )
                )

    def acyclic(actions, base_action, current_action=None, stack=None):
        """Check for circular or misspelled dependencies."""
        stack = stack or []
        current_action = current_action or base_action

        stack.append(current_action.name)
        for dep in current_action.requires:
            if dep == base_action.name and len(stack) > 0:
                return ' -> '.join(stack)

            cycle = acyclic(actions, base_action, actions[dep], stack)
            if cycle:
                return cycle

        stack.pop()

    for _, action in actions.items():
        cycle = acyclic(actions, action)
        if cycle:
            return (
                False,
                "`actions` contains circular dependency: {}".format(cycle)
            )

    return (True, "all ok")


class Job(ConfigRecord):
    # required
    name = field(type=str, mandatory=True, invariant=inv_name_identifier)
    node = field(type=str, mandatory=True, invariant=inv_identifier)
    schedule = field(
        mandatory=True,
        factory=schedule_parse.ConfigGenericSchedule.from_config
    )
    actions = field(
        type=ActionMap,
        mandatory=True,
        invariant=inv_actions,
        factory=ActionMap.from_config
    )
    namespace = field(type=str, mandatory=True)

    monitoring = field(type=PMap, initial=m(), factory=pmap)
    queueing = field(type=bool, initial=True)
    run_limit = field(type=int, initial=50)
    all_nodes = field(type=bool, initial=False)
    cleanup_action = field(
        type=(Action, type(None)), initial=None, factory=Action.from_config
    )
    enabled = field(type=bool, initial=True)
    allow_overlap = field(type=bool, initial=False)
    max_runtime = field(
        type=(datetime.timedelta, type(None)),
        factory=config_utils.valid_time_delta,
        initial=None
    )
    time_zone = field(
        type=(datetime.tzinfo, type(None)),
        initial=None,
        factory=lambda tz: tz if isinstance(tz, (datetime.tzinfo, type(None))) else pytz.timezone(tz)
    )
    expected_runtime = field(
        type=(datetime.timedelta, type(None)),
        factory=config_utils.valid_time_delta,
        initial=datetime.timedelta(hours=24)
    )

    def __invariant__(self):
        if CLEANUP_ACTION_NAME in self.actions:
            return (False, "actions.cleanup reserved for cleanup action")

        if self.cleanup_action and \
                self.cleanup_action.name != CLEANUP_ACTION_NAME:
            return (False, "cleanup_action cannot have name")

        return (True, "all ok")

    @classmethod
    def from_config(kls, job):
        """ Create Job instance from raw JSON/YAML data.
        """
        try:
            job = dict(**job)

            cleanup_action = job.get('cleanup_action')
            if cleanup_action is not None:
                cleanup_action.setdefault('name', CLEANUP_ACTION_NAME)

            return kls.create(job)
        except Exception as e:
            raise ValueError(f"jobs {job.get('name', 'unnamed')} {e}"
                             ).with_traceback(e.__traceback__)


class JobMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = Job

    @staticmethod
    def from_config(jobs):
        if jobs is None or isinstance(jobs, JobMap):
            return jobs

        if isinstance(jobs, list):
            nameset = set()
            for j in jobs:
                if j['name'] in nameset:
                    raise ValueError("duplicate name found: {}", j['name'])
                else:
                    nameset.add(j['name'])
            jobs = {j['name']: j for j in jobs}

        return JobMap.create({n: Job.from_config(j) for n, j in jobs.items()})
