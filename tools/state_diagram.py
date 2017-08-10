"""
 Create a graphviz state diagram from a tron.util.state.NamedEventState.

 Running this script will create two graphviz files:
    action.dot
    service.dot

"""
from __future__ import absolute_import
from __future__ import unicode_literals

from tron.core.actionrun import ActionRun
from tron.core.service import ServiceInstance


def traverse_graph(starting_state, func=lambda f, a, t: None, seen_states=None):
    """Traverse the graph depth-first without cycling."""
    seen_states = seen_states or []
    seen_states.append(starting_state)

    for action, state in starting_state.iteritems():
        func(starting_state, action, state)
        if state in seen_states:
            continue
        traverse_graph(state, func, seen_states)

    return seen_states


def build_diagram(states, starting_state):
    """Build the diagram."""

    def build_node(state):
        return '%s[label="state: %s"];' % (
            state.name,
            state.name,
        )

    def build_edges(starting_state):
        edges = []

        def collection_edges(from_state, act, to_state):
            edges.append((from_state.name, act, to_state.name))
        traverse_graph(starting_state, collection_edges)

        for edge in edges:
            yield '%s -> %s[label="%s"];' % (edge[0], edge[2], edge[1])

    return "digraph g{%s\n%s}" % (
        '\n'.join(build_node(state) for state in states),
        '\n'.join(build_edges(starting_state)),
    )


def dot_from_starting_state(starting_state):
    state_data = traverse_graph(starting_state)
    return build_diagram(state_data, starting_state)


machines = {
    'action':           ActionRun.STATE_SCHEDULED,
    'service_instance': ServiceInstance.STATE_DOWN,
}

if __name__ == "__main__":
    for name, starting_state in machines.iteritems():
        with open("%s.dot" % name, 'w') as f:
            f.write(dot_from_starting_state(starting_state))
