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

    def __init__(self, config_container):
        """ Build an adjacency list and a reverse adjacency list for the graph,
        and store all the actions as well as which actions belong to which job
        """
        self.action_map = {}
        self._actions_for_job = defaultdict(list)
        self._adj_list = defaultdict(list)
        self._rev_adj_list = defaultdict(list)

        for job_name, job_config in config_container.get_jobs().items():
            for action_name, action_config in job_config.actions.items():
                full_name = self._save_action(action_name, job_name, action_config)

                if action_config.requires:
                    self._rev_adj_list[full_name] += [
                        AdjListEntry(f'{job_name}.{required_action}', False)
                        for required_action in action_config.requires
                    ]
                if action_config.triggered_by:
                    self._rev_adj_list[full_name] += [
                        AdjListEntry('.'.join(trigger.split('.')[:3]), True)
                        for trigger in action_config.triggered_by
                    ]

                for parent_action, is_trigger in self._rev_adj_list[full_name]:
                    self._adj_list[parent_action].append(AdjListEntry(full_name, is_trigger))

            cleanup_action_config = job_config.cleanup_action
            if cleanup_action_config:
                self._save_action(cleanup_action_config.name, job_name, cleanup_action_config)

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
