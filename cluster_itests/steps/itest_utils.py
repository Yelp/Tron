import subprocess


def run(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    returncode = process.returncode
    output = "".join([line.decode('utf8') for line in process.stdout.readlines()])
    return returncode, output
