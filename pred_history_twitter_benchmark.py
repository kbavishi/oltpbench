#! /usr/bin/env python

import os
import sys
import subprocess
import shlex
import argparse
from termcolor import cprint

POSTGRES_HOST = None
POSTGRES_IP = None

def create_dir_if_not_exists(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)

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

def generate_twitter_config(sched_policy, pred_history, arrival_rate=75,
                            alpha=0.5, gedf_factor=0.4, timeout=300,
                            fixed_deadline="false"):
    os.environ["POSTGRES_IP"] = POSTGRES_IP
    os.environ["SCHED_POLICY"] = sched_policy
    os.environ["PRED_HISTORY"] = str(pred_history)
    os.environ["RATE"] = str(arrival_rate)
    os.environ["ALPHA"] = "%.2f" % alpha
    os.environ["GEDF_FACTOR"] = "%.2f" % gedf_factor
    os.environ["TIMEOUT"] = str(timeout)
    os.environ["FIXED_DEADLINE"] = fixed_deadline

    run_bash_cmd("j2 config/twitter_config.xml.j2 | "
                 "tee config/twitter_config.xml")

def run_twitter_benchmark(sched_policy, output_file, csv_file, iterations=11,
                          pred_history=0, arrival_rate=75, alpha=0.5,
                          gedf_factor=0.4, fixed_deadline="false"):
    generate_twitter_config(sched_policy, pred_history,
                            arrival_rate=arrival_rate, alpha=alpha,
                            gedf_factor=gedf_factor,
                            fixed_deadline=fixed_deadline)

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

def main(arrival_rate, iterations, alpha, gedf_factor, fixed_deadline):
    print "CURRENTLY TESTING: %s, %s, %s" % (arrival_rate, alpha, gedf_factor)
    run_twitter_benchmark("fifo", "output/fifo", "fifo",
                          iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)
    run_twitter_benchmark("edf", "output/edf", "edf",
                          iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)

    # PLA EDF tests with different history sizes
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_10", "edf_loc_10",
                          pred_history=10, iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_100", "edf_loc_100",
                          pred_history=100, iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)
    run_twitter_benchmark("edf_pred_loc", "output/edf_loc_1000", "edf_loc_1000",
                          pred_history=1000, iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)

    # PLA gEDF tests with different history sizes
    run_twitter_benchmark("gedf_pred_loc", "output/gedf_loc_10", "gedf_loc_10",
                          pred_history=10, iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)
    run_twitter_benchmark("gedf_pred_loc", "output/gedf_loc_100", "gedf_loc_100",
                          pred_history=100, iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)
    run_twitter_benchmark("gedf_pred_loc", "output/gedf_loc_1000", "gedf_loc_1000",
                          pred_history=1000, iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)

    # PLA Buf-Loc EDF and gEDF
    run_twitter_benchmark("edf_pred_buf_loc", "output/edf_pred_buf_loc",
                          "edf_pred_buf_loc", iterations=iterations,
                          arrival_rate=arrival_rate,
                          alpha=alpha, gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)
    run_twitter_benchmark("gedf_pred_buf_loc", "output/gedf_pred_buf_loc",
                          "gedf_pred_buf_loc", iterations=iterations,
                          arrival_rate=arrival_rate,
                          alpha=alpha, gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline)

if __name__ == '__main__':
    # Create necessary directories
    create_dir_if_not_exists("output")
    create_dir_if_not_exists("results")
    create_dir_if_not_exists("new_pred_data")

    parser = argparse.ArgumentParser(description='Run EDF tests')
    parser.add_argument('postgres_ip', metavar='IP_ADDR', type=str,
                        help='IP address of the Postgres instance')
    parser.add_argument('--rate', type=int, default=75,
                        help='Query arrival rate in reqs/sec')
    parser.add_argument('--alpha', type=str, default='0.5',
                        help='Smoothing factor for EWMA')
    parser.add_argument('--gedf_factor', type=str, default='0.4',
                        help='gEDF factor for deadline grouping')
    parser.add_argument('--iter', type=int, default=11,
                        help='Number of times to repeat experiment')
    parser.add_argument('--fixed_deadline', type=str, default="false",
                        help="Choose a fixed deadline for queries")

    args = parser.parse_args()

    # Update host information
    global POSTGRES_HOST, POSTGRES_IP
    POSTGRES_IP = args.postgres_ip
    POSTGRES_HOST = "vagrant@%s" % POSTGRES_IP

    alpha = float(args.alpha)
    assert 0.0 <= alpha <= 1.0, "Incorrect alpha value: %s" % alpha
    gedf_factor = float(args.gedf_factor)
    assert 0.0 <= gedf_factor <= 1.0, "Incorrect gedf value: %s" % gedf_factor

    main(args.rate, args.iter, alpha, gedf_factor, args.fixed_deadline)

    # Rename output and results directory for backups
    parent_dir = "new_pred_data/ph_%s_alpha_%s_gedf_%s" % (args.rate, alpha,
                                                           gedf_factor)
    os.renames("output", "%s/output" % parent_dir)
    os.renames("results", "%s/results" % parent_dir)
