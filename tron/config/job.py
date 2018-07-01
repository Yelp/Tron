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
from tron.config.schema import MASTER_NAMESPACE
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


def inv_acyclic(actions):
    def inv_acyclic_rec(actions, base_action, current_action=None, stack=None):
        """Check for circular or misspelled dependencies."""
        stack = stack or []
        current_action = current_action or base_action

        stack.append(current_action.name)
        for dep in current_action.requires:
            if dep == base_action.name and len(stack) > 0:
                return ' -> '.join(stack)

            cycle = inv_acyclic_rec(actions, base_action, actions[dep], stack)
            if cycle:
                return cycle

        stack.pop()

    for _, action in actions.items():
        cycle = inv_acyclic_rec(actions, action)
        if cycle:
            return (False, "graph contains cycles: {}".format(cycle))

    return (True, "no cycles detected")


def inv_no_external_deps(actions):
    for an, av in actions.items():
        for dep in av.requires:
            if dep not in actions:
                return (
                    False,
                    "external dependency detected: {} -> {}".format(an, dep)
                )

    return (True, "no external dependencies")


class Job(ConfigRecord):
    # required
    name = field(type=str, mandatory=True, invariant=inv_name_identifier)
    node = field(type=str, mandatory=True, invariant=inv_identifier)
    schedule = field(
        mandatory=True,
        factory=lambda s: schedule_parse.valid_schedule(s, None)
    )
    actions = field(
        type=ActionMap,
        mandatory=True,
        invariant=lambda ac: (
            (len(ac) > 0, "can't be empty"),
            inv_no_external_deps(ac),
            inv_acyclic(ac),
        )
    )
    namespace = field(type=str, mandatory=True)

    monitoring = field(type=PMap, initial=m(), factory=pmap)
    queueing = field(type=bool, initial=True)
    run_limit = field(type=int, initial=50)
    all_nodes = field(type=bool, initial=False)
    cleanup_action = field(type=(Action, type(None)), initial=None)
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
        factory=lambda tz: tz if tz is None else pytz.timezone(tz)
    )
    expected_runtime = field(
        type=(datetime.timedelta, type(None)),
        factory=config_utils.valid_time_delta,
        initial=datetime.timedelta(hours=24)
    )

    @classmethod
    def from_config(kls, job, context):
        """ Create Job instance from raw JSON/YAML data.
        """
        try:
            job = dict(**job)

            if not context.partial and job['node'] not in context.nodes:
                raise ValueError("Unknown node name {}".format(job['node']))

            job['actions'] = ActionMap.from_config(job['actions'], context)
            if job.get('cleanup_action') is not None:
                job['cleanup_action'] = Action.from_config(
                    dict(name=CLEANUP_ACTION_NAME, **job['cleanup_action']),
                    context
                )

            if 'namespace' not in job or not job['namespace']:
                job['namespace'] = context.namespace or MASTER_NAMESPACE

            if not context.partial:
                job['name'] = '{}.{}'.format(job['namespace'], job['name'])

            return kls.create(job)
        except Exception as e:
            raise ValueError(
                "Failed to create job '{}' at {}: {}".format(
                    job.get('name'), context.path, e
                )
            ).with_traceback(e.__traceback__)


class JobMap(CheckedPMap):
    __key_type__ = str
    __value_type__ = Job

    @staticmethod
    def from_config(jobs, context):
        nameset = set()
        for j in jobs:
            if j['name'] in nameset:
                raise ValueError("duplicate name found: {}", j['name'])
            else:
                nameset.add(j['name'])

        return JobMap.create({
            j['name']: Job.from_config(j, context)
            for j in jobs
        })
