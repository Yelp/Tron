#!/usr/bin/env python
import argparse
import os

import yaml

DST_DIR = "/tmp/tron-servdir"
SRC_DIR = "/nail/etc/services/tron/prod"


def parse_args():
    parser = argparse.ArgumentParser(
        description='Creating namespaces and jobs configuration from Tron prod'
    )
    parser.add_argument(
        '--multiple',
        type=int,
        default=1,
        help='multiple workload of namespaces and jobs from Tron prod'
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    for filename in os.listdir(SRC_DIR):
        print('filename = {}'.format(filename))
        filepath = os.path.join(SRC_DIR, filename)
        if os.path.isfile(filepath) and filepath.endswith(".yaml"):
            with open(filepath, "r") as f:
                config = yaml.load(f)
                if filename == "MASTER.yaml":
                    for key, _ in config.items():
                        if key != 'jobs':
                            del config[key]

                jobs = config.get("jobs", [])
                if jobs is not None:
                    for job in jobs:
                        job['node'] = "localhost"
                        if 'monitoring' in job:
                            del job['monitoring']
                        for action in job.get("actions", []):
                            action['command'] = 'sleep 10s'
                            if "node" in action:
                                action['node'] = "localhost"
            for i in range(args.multiple):
                out_filepath = os.path.join(
                    DST_DIR, 'prod_' + str(i) + '-' + filename
                )
                with open(out_filepath, 'w') as outf:
                    yaml.dump(config, outf, default_flow_style=False)


if __name__ == '__main__':
    main()
