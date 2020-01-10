from collections import defaultdict
from collections import namedtuple

from tron.core.action import Action
from tron.core.actiongraph import ActionGraph
from tron.utils import maybe_decode

AdjListEntry = namedtuple('AdjListEntry', ['action_name', 'is_trigger'])


class JobGraph(object):
    """ A JobGraph stores the entire DAG of jobs and actions, including
    cross-job dependencies (aka triggers)
    """

    def __init__(self, config_container, should_validate_missing_dependency=False):
        """ Build an adjacency list and a reverse adjacency list for the graph,
        and store all the actions as well as which actions belong to which job
        """
        self.action_map = {}
        self._actions_for_job = defaultdict(list)
        self._adj_list = defaultdict(list)
        self._rev_adj_list = defaultdict(list)

        all_actions = set()
        for job_name, job_config in config_container.get_jobs().items():
            for action_name, action_config in job_config.actions.items():
                full_name = self._save_action(action_name, job_name, action_config)
                all_actions.add(full_name)

                for required_action in action_config.requires or []:
                    required_action_name = f'{job_name}.{required_action}'
                    self._rev_adj_list[full_name].append(AdjListEntry(required_action_name, False))

                for trigger in action_config.triggered_by or []:
                    trigger_action_name = '.'.join(trigger.split('.')[:3])
                    self._rev_adj_list[full_name].append(AdjListEntry(trigger_action_name, True))

                for parent_action, is_trigger in self._rev_adj_list[full_name]:
                    self._adj_list[parent_action].append(AdjListEntry(full_name, is_trigger))

            cleanup_action_config = job_config.cleanup_action
            if cleanup_action_config:
                self._save_action(cleanup_action_config.name, job_name, cleanup_action_config)

        if should_validate_missing_dependency:
            missing_dependent_actions = defaultdict(list)
            for action_name in self._rev_adj_list:
                for dependent_action_entry in self._rev_adj_list[action_name]:
                    if dependent_action_entry.action_name not in all_actions:
                        missing_dependent_actions[dependent_action_entry.action_name].append(action_name)

            error_messages = []
            for action_name, child_action_names in missing_dependent_actions.items():
                error_messages.append(
                    'Action {0} is dependency of actions:\n{1}'.format(
                        action_name,
                        '\n'.join(
                            ['  - {}'.format(child_action_name) for child_action_name in child_action_names]
                        )
                    )
                )

            if error_messages:
                raise ValueError(
                    (
                        'The following actions are dependencies of other actions but missing:\n'
                        '{0}\n'
                        'Please check if you have deleted/renamed any of them or their containing jobs.'
                    ).format(
                        '\n'.join(error_messages),
                    )
                )

    def get_action_graph_for_job(self, job_name):
        """ Traverse the JobGraph for a specific job to construct an ActionGraph for it """
        job_action_map = {}
        required_actions, required_triggers = defaultdict(set), defaultdict(set)

        for action_name in self._actions_for_job[job_name]:
            # Any actions that belong to _this job_ are not prefixed by the job name
            short_action_name = action_name.split('.')[-1]
            job_action_map[short_action_name] = self.action_map[action_name]
            required_actions[short_action_name] = {
                entry.action_name.split('.')[-1]
                for entry in self._rev_adj_list[action_name]
                if not entry.is_trigger
            }

            # We call this twice to build the complete DAG for the job; the first time
            # we search the forward adjacency list and the second time we search the
            # reverse adjancency list.  This ensures we don't miss any triggers
            required_triggers = self._get_required_triggers(action_name, required_triggers)
            required_triggers = self._get_required_triggers(action_name, required_triggers, search_up=False)
        return ActionGraph(job_action_map, required_actions, required_triggers)

    def _save_action(self, action_name, job_name, config):
        action_name = maybe_decode(action_name)
        full_name = f'{job_name}.{action_name}'
        self.action_map[full_name] = Action.from_config(config)
        self._actions_for_job[job_name].append(full_name)
        return full_name

    def _get_required_triggers(self, action_name, triggers, search_up=True):
        stack = [action_name]
        visited = set()

        # Do DFS to search the adjacency list and find all of the required triggers
        # for a particular action
        while stack:
            current_action = stack.pop()
            visited.add(current_action)
            adj_list = self._rev_adj_list if search_up else self._adj_list
            for next_action, is_trigger in adj_list[current_action]:
                if not is_trigger:
                    continue

                if next_action not in visited:
                    stack.append(next_action)

                if current_action == action_name:
                    current_action = current_action.split('.')[-1]

                if search_up:
                    triggers[current_action].add(next_action)
                else:
                    triggers[next_action].add(current_action)

        return triggers
