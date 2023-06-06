#!/usr/bin/env python
""" This script is for load testing of Tron

Historically, Tronview and Tronweb were (are) slow. To better understand the performance
bottleneck of Tron, we could use this script to  generate the fake namespaces and
jobs as many as we want to perform load testing. Ticket TRON-70 tracks the progress
of speeding up Tronview and Tronweb.
"""
import argparse
import os

from tron import yaml


def parse_args():
    parser = argparse.ArgumentParser(
        description="Creating namespaces and jobs configuration for load testing",
    )
    parser.add_argument(
        "--multiple",
        type=int,
        default=1,
        help="multiple workload of namespaces and jobs from source directory",
    )
    parser.add_argument(
        "--src",
        default="/nail/etc/services/tron/prod",
        help="Directory to get Tron configuration files",
    )
    parser.add_argument(
        "--dest",
        default="/tmp/tron-servdir",
        help="Directory to put Tron configuration files for load testing",
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    for filename in os.listdir(args.src):
        print(f"filename = {filename}")
        filepath = os.path.join(args.src, filename)
        if os.path.isfile(filepath) and filepath.endswith(".yaml"):
            with open(filepath) as f:
                config = yaml.load(f)

            if filename == "MASTER.yaml":
                for key in list(config):
                    if key != "jobs":
                        del config[key]

            jobs = config.get("jobs", [])
            if jobs is not None:
                for job in jobs:
                    job["node"] = "localhost"
                    if "monitoring" in job:
                        del job["monitoring"]
                    for action in job.get("actions", []):
                        action["command"] = "sleep 10s"
                        if "node" in action:
                            action["node"] = "localhost"
            for i in range(args.multiple):
                out_filepath = os.path.join(
                    args.dest,
                    "load_testing_" + str(i) + "-" + filename,
                )
                with open(out_filepath, "w") as outf:
                    yaml.dump(config, outf, default_flow_style=False)


if __name__ == "__main__":
    main()
