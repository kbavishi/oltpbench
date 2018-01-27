#! /usr/bin/env python

import os
import sys
import subprocess
import shlex
from termcolor import cprint

POSTGRES_HOST = None
POSTGRES_IP = None

def run_bash_cmd(cmd):
    cprint(cmd, 'yellow', attrs=['bold'])
    if '>' in cmd or '>>' in cmd or '|' in cmd:
        # Subprocess module does not handle bash operators cleanly
        return subprocess.check_output(["bash", "-c", cmd])
    else:
        return subprocess.check_output(shlex.split(cmd))

def restart_postgres():
    run_bash_cmd("ssh -p 8022 -i /home/karan/.keys/vm_key "
                 "%s ./oltpbench/reset_postgres.sh" % POSTGRES_HOST)

def build_branch(branch_name):
    run_bash_cmd("git checkout %s" % branch_name)
    run_bash_cmd("ant")

def trim_first_line(csv_file):
    run_bash_cmd("tail -n +2 %s > /tmp/x.csv" % csv_file)
    run_bash_cmd("mv /tmp/x.csv %s" % csv_file)

def generate_twitter_config(sched_policy, pred_history, arrival_rate=75):
    os.environ["POSTGRES_IP"] = POSTGRES_IP
    os.environ["SCHED_POLICY"] = sched_policy
    os.environ["PRED_HISTORY"] = str(pred_history)
    os.environ["RATE"] = str(arrival_rate)

    run_bash_cmd("j2 config/twitter_config.xml.j2 | "
                 "tee config/twitter_config.xml")

def run_twitter_benchmark(sched_policy, output_file, csv_file, iterations=11,
                          pred_history=5):
    generate_twitter_config(sched_policy, pred_history)

    for i in xrange(iterations):
        # Always restart Postgres after each run
        restart_postgres()
        restart_postgres()

        output = run_bash_cmd("./oltpbenchmark -b twitter "
                              "-c config/twitter_config.xml "
                              "--execute=true --histograms --output %s.%s" %
                              (csv_file, i))

        # Need to trim first line of CSV file for our stats gathering scripts
        trim_first_line("results/%s.%s.csv" % (csv_file, i))

        open("%s.%s.txt" % (output_file, i), "w").write(output)

def main():
    run_twitter_benchmark("fifo", "output/fifo", "fifo")
    run_twitter_benchmark("edf", "output/edf", "edf")
    run_twitter_benchmark("edf_pred_loc_old", "output/edf_loc_old", "edf_loc_old",
                          pred_history=5)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_10", "edf_loc_10",
                          pred_history=10)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_100", "edf_loc_100",
                          pred_history=100)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_500", "edf_loc_500",
                          pred_history=500)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_1000", "edf_loc_1000",
                          pred_history=1000)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_5000", "edf_loc_5000",
                          pred_history=5000)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Incorrect number of arguments"
        sys.exit(1)

    # Update host information
    global POSTGRES_HOST, POSTGRES_IP
    POSTGRES_IP = sys.argv[1]
    POSTGRES_HOST = "vagrant@%s" % POSTGRES_IP

    main()
