#!/usr/bin/python
import sys
import yaml

ACTION_RUN_SCHEDULED = 0
ACTION_RUN_QUEUED = 1
ACTION_RUN_CANCELLED = 2
ACTION_RUN_UNKNOWN = 3
ACTION_RUN_RUNNING = 4
ACTION_RUN_FAILED = 10
ACTION_RUN_SUCCEEDED = 11

state_num_to_string = {
    0: 'scheduled',
    1: 'queued',
    2: 'cancelled',
    3: 'unknown',
    4: 'running',
    10: 'failed',
    11: 'succeeded',
}

if __name__ == "__main__":
    state_data = yaml.load(sys.stdin)
    if state_data['version'] == (0,1,10):
        state_data['version'] = (0,2,0)
        for job_data in state_data['jobs'].itervalues():
            for run in job_data['runs']:
                for action_run in run['runs']:
                    if 'state' in action_run:
                        action_run['state'] = state_num_to_string[action_run['state']]

        yaml.dump(state_data, sys.stdout, default_flow_style=False, indent=4)
    else:
        print "Invalid version: %r" % (state_data['version'],)
        sys.exit(1)
