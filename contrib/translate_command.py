#!/usr/bin/env python
"""
This script is for translating command from % string to format string
"""
import argparse
import os
from subprocess import PIPE
from subprocess import Popen

MASTER_TEST_FILE = "test_master.yaml"
TRANSLATE_PREFIX = "formatstr-"


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Translate command from % string to format string'
    )
    parser.add_argument(
        'dirname',
        help='source file'
    )
    args = parser.parse_args()
    return args


def translate_command(command):
    new_command = command.replace("%(", "{").replace(")s", "}")
    return new_command


def remove_escape(command):
    new_command = command.replace("%%", "%")
    return new_command


def rollback(command):
    new_command = command.replace("%", "%%")
    return new_command


def translate_file(input_filepath, output_filepath):
    with open(input_filepath, "r") as f:
        with open(output_filepath, 'w') as outf:
            line = f.readline()
            while line:
                # newline = translate_command(line)
                # newline = remove_escape(line)
                newline = rollback(line)
                outf.write(newline)
                line = f.readline()


def create_master_jobs_file(input_filepath, output_filepath):
    jobs_flag = False
    with open(input_filepath, "r") as f:
        with open(output_filepath, 'w') as outf:
            line = f.readline()
            while line:
                if line.startswith("jobs:"):
                    jobs_flag = True
                if jobs_flag is True:
                    outf.write(line)
                line = f.readline()


def validate_files(file1, file2, master_filepath):
    p1 = Popen(["tronfig", "-V", file1, "-m", master_filepath], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output1, err1 = p1.communicate(b"input data that is passed to subprocess' stdin")
    p2 = Popen(["tronfig", "-V", file2, "-m", master_filepath], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output2, err2 = p2.communicate(b"input data that is passed to subprocess' stdin")
    if(output1 == output2):
        return True

    line1 = output1.splitlines()
    line2 = output2.splitlines()
    for i in range(len(line1)):
        if line1[i] != line2[i]:
            print("At line {}\n{}\n not equal to \n{}\n".format(i, line1[i], line2[i]))
    return False


def main():
    args = parse_args()
    filenames = [f for f in os.listdir(args.dirname)]
    for filename in filenames:
        if filename.startswith(TRANSLATE_PREFIX) is False and filename.endswith(".yaml"):
            master_mode = False
            if filename == "MASTER.yaml":
                master_mode = True
            filedir = args.dirname
            input_filepath = os.path.join(args.dirname, filename)
            output_filepath = os.path.join(filedir, TRANSLATE_PREFIX + filename)
            master_filepath = os.path.join(filedir, "MASTER.yaml")

            translate_file(input_filepath, output_filepath)
            if master_mode is True:
                cp_input_filepath = os.path.join(args.dirname, MASTER_TEST_FILE)
                create_master_jobs_file(master_filepath, cp_input_filepath)
                cp_output_filepath = os.path.join(filedir, TRANSLATE_PREFIX + MASTER_TEST_FILE)
                translate_file(cp_input_filepath, cp_output_filepath)

            # validate if the files are the same
            if master_mode is False:
                validate_result = validate_files(input_filepath, output_filepath, master_filepath)
            else:
                validate_result = validate_files(cp_input_filepath, cp_output_filepath, master_filepath)
            if validate_result is True:
                print(bcolors.OKGREEN + "Validate {} ... successful".format(filename) + bcolors.ENDC)
                os.rename(output_filepath, input_filepath)
                if master_mode is True:  # Remove the temporary files
                    os.remove(cp_input_filepath)
                    os.remove(cp_output_filepath)
            else:
                print(bcolors.FAIL + "Validate {} ... failed".format(filename) + bcolors.ENDC)


if __name__ == '__main__':
    main()
