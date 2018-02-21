#! /usr/bin/env python

import os
import sys
import subprocess
import shlex
import argparse
import random
import shutil
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
    run_bash_cmd("ssh -p 8022 -i /users/kbavishi/.vagrant/machines/default/virtualbox/private_key "
                 "%s ./oltpbench/reset_postgres.sh" % POSTGRES_HOST)

def build_branch(branch_name):
    run_bash_cmd("git checkout %s" % branch_name)
    run_bash_cmd("ant")

def trim_first_line(csv_file):
    run_bash_cmd("tail -n +2 %s > /tmp/x.csv" % csv_file)
    run_bash_cmd("mv /tmp/x.csv %s" % csv_file)

def generate_twitter_config(sched_policy, pred_history, arrival_rate=75,
                            alpha=0.5, gedf_factor=0.4, timeout=300,
                            fixed_deadline="false", random_page_cost=4.0,
                            num_bins=200, buffer_size=750*1024*1024/8192,
                            bin_window_threshold=25*300):
    os.environ["POSTGRES_IP"] = POSTGRES_IP
    os.environ["SCHED_POLICY"] = sched_policy
    os.environ["PRED_HISTORY"] = str(pred_history)
    os.environ["RATE"] = str(arrival_rate)
    os.environ["ALPHA"] = "%.2f" % alpha
    os.environ["GEDF_FACTOR"] = "%.2f" % gedf_factor
    os.environ["TIMEOUT"] = str(timeout)
    os.environ["FIXED_DEADLINE"] = fixed_deadline
    os.environ["RANDOM_PAGE_COST"] = "%.2f" % random_page_cost
    os.environ["NUM_BINS"] = str(num_bins)
    os.environ["BUFFER_SIZE"] = str(buffer_size)
    os.environ["BIN_WINDOW_THRESHOLD"] = str(bin_window_threshold)

    run_bash_cmd("j2 config/twitter_config.xml.j2 | "
                 "tee config/twitter_config.xml")

def run_twitter_benchmark(sched_policy, output_file, csv_file, iterations=11,
                          pred_history=0, arrival_rate=75, alpha=0.5,
                          gedf_factor=0.4, timeout=720,
                          fixed_deadline="false", random_page_cost=4.0):
    generate_twitter_config(sched_policy, pred_history,
                            arrival_rate=arrival_rate, alpha=alpha,
                            gedf_factor=gedf_factor, timeout=timeout,
                            fixed_deadline=fixed_deadline,
                            random_page_cost=random_page_cost)

    # We will modify the buffer_stats.txt file and fill it up with weird values
    # before running the test. Later, the dynamic prob calculation should kick
    # in and correct the values
    stats_file = os.path.join(os.environ.get("HOME"), "buffer_stats.txt")
    lines = open(stats_file, "r").readlines()

    # Create a copy of the original file before overwriting it
    shutil.copyfile(stats_file, "/tmp/buffer_stats.txt")

    new_lines = []
    for i in xrange(4, len(lines)):
        pred, prob = lines[i].split()
        new_lines += ["%s %s" % (random.randint(1, 150000), prob)]

    lines = lines[:4] + new_lines
    with open(stats_file, "w") as f:
        for line in lines:
            f.write("%s\n" % line)


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

    # Restore buffer stats file
    shutil.copyfile("/tmp/buffer_stats.txt", stats_file)

def main(arrival_rate, iterations, alpha, gedf_factor, fixed_deadline,
         random_page_cost):
    print "CURRENTLY TESTING: %s, %s, %s" % (arrival_rate, alpha, gedf_factor)
    run_twitter_benchmark("fifo", "output/fifo", "fifo",
                          iterations=iterations,
                          arrival_rate=arrival_rate, alpha=alpha,
                          gedf_factor=gedf_factor,
                          fixed_deadline=fixed_deadline,
                          random_page_cost=random_page_cost)


if __name__ == '__main__':
    # Create necessary directories
    create_dir_if_not_exists("output")
    create_dir_if_not_exists("results")
    create_dir_if_not_exists("dyn_pred_data")

    parser = argparse.ArgumentParser(description='Run EDF tests')
    parser.add_argument('postgres_ip', metavar='IP_ADDR', type=str,
                        help='IP address of the Postgres instance')
    parser.add_argument('--rate', type=int, default=75,
                        help='Query arrival rate in reqs/sec')
    parser.add_argument('--alpha', type=str, default='0.5',
                        help='Smoothing factor for EWMA')
    parser.add_argument('--gedf_factor', type=str, default='0.4',
                        help='gEDF factor for deadline grouping')
    parser.add_argument('--random_page_cost', type=str, default='4.0',
                        help='Random I/O cost for Postgres')
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
    random_page_cost = float(args.random_page_cost)
    assert random_page_cost >= 0.0 , \
        "Incorrect random page cost value: %s" % random_page_cost

    main(args.rate, args.iter, alpha, gedf_factor, args.fixed_deadline,
         random_page_cost)

    # Rename output and results directory for backups
    deadline = "nfd" if args.fixed_deadline == "false" else "fd"
    parent_dir = "dyn_pred_data/ph_%s_alpha_%s_gedf_%s_%s" % (args.rate, alpha,
                                                              gedf_factor,
                                                              deadline)
    os.renames("output", "%s/output" % parent_dir)
    os.renames("results", "%s/results" % parent_dir)
