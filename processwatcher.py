#!/home/stephan/.virtualenvs/nntp/bin/python

import subprocess

cmd = "/usr/bin/top -bc -n 1 | grep python"


proc = subprocess.Popen(['top', '-n 1'], shell=False, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
if proc:
    ss = proc.stdout.readlines()
    for s in ss:
        print(s.decode("utf-8"))
