import subprocess


def run(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    returncode = process.returncode
    output = "".join([l.decode('utf8') for l in process.stdout.readlines()])
    return returncode, output
