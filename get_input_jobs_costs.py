#! /usr/bin/env python

import os
import psycopg2
import subprocess
import shlex
import sys

LIMIT_FOLLOWERS = 400
LIMIT_TWEETS_FOR_UID = 10

COST_FILE = os.path.join(os.getcwd(), "input_jobs_cost.txt")

def get_plan_cost(output):
    for line in output.split("\n"):
        line = line.strip()
        if not line.startswith("Total Cost"):
            continue
        cost = float(line.split(":")[1])
        return cost

    assert False, "No cost found"

def get_tweet(cur, tid):
    cur.execute("EXPLAIN (FORMAT YAML) "
                "SELECT * FROM tweets WHERE id = %s;" % tid)
    output = cur.fetchall()

    return get_plan_cost(output[0][0])

def get_tweets_from_following(cur, uid):
    cost = 0
    cur.execute("EXPLAIN (FORMAT YAML) "
                "SELECT f2 FROM follows WHERE f1 = %s LIMIT %s;" %
                (uid, LIMIT_FOLLOWERS))
    output = cur.fetchall()
    cost += get_plan_cost(output[0][0])

    cur.execute("SELECT f2 FROM follows WHERE f1 = %s LIMIT %s;" %
                (uid, LIMIT_FOLLOWERS))
    uids = cur.fetchall()
    uid_list = map(str, [u[0] for u in uids])

    if uid_list:
        cur.execute("EXPLAIN (FORMAT YAML) "
                    "SELECT * FROM tweets WHERE uid IN (%s);" % ",".join(uid_list))
        output = cur.fetchall()
        cost += get_plan_cost(output[0][0])

    return cost

def get_followers(cur, uid):
    cost = 0
    cur.execute("EXPLAIN (FORMAT YAML) "
                "SELECT f2 FROM followers WHERE f1 = %s LIMIT %s;" %
                (uid, LIMIT_FOLLOWERS))
    output = cur.fetchall()
    cost += get_plan_cost(output[0][0])

    cur.execute("SELECT f2 FROM followers WHERE f1 = %s LIMIT %s;" %
                (uid, LIMIT_FOLLOWERS))
    uids = cur.fetchall()
    uid_list = map(str, [u[0] for u in uids])
    
    if uid_list:
        cur.execute("EXPLAIN (FORMAT YAML) "
                    "SELECT uid, name FROM user_profiles "
                    "WHERE uid IN (%s);" % ",".join(uid_list))
        output = cur.fetchall()
        cost += get_plan_cost(output[0][0])

    return cost

def get_user_tweets(cur, uid):
    cur.execute("EXPLAIN (FORMAT YAML) "
                "SELECT * FROM tweets WHERE uid = %s LIMIT %s;" %
                (uid, LIMIT_TWEETS_FOR_UID))
    output = cur.fetchall()

    return get_plan_cost(output[0][0])

def insert_tweet(cur, uid):
    start_time = monotonic_time()

    cur.execute("SELECT * FROM tweets WHERE uid = %s LIMIT %s;" %
                (uid, LIMIT_TWEETS_FOR_UID))
    cur.fetchall()

    end_time = monotonic_time()
    return end_time - start_time

def run_bash_cmd(cmd):
    subprocess.check_output(shlex.split(cmd))

def restart_postgres():
    run_bash_cmd("sudo service postgresql stop")
    run_bash_cmd("echo 3 | sudo tee /proc/sys/vm/drop_caches")
    run_bash_cmd("sudo service postgresql start")

def run_transaction(cur, trans_type, num):
    if trans_type == 1:
        return get_tweet(cur, num)
    elif trans_type == 2:
        return get_tweets_from_following(cur, num)
    elif trans_type == 3:
        return get_followers(cur, num)
    elif trans_type == 4:
        return get_user_tweets(cur, num)
    elif trans_type == 5:
        #return insert_tweet(num)
        # XXX - What to do with insertTweet?
        return 100.0
    else:
        assert False, "Unknown transaction type: %s" % trans_type

def read_input_file(cur, filepath, output_filepath, limit=2000000):
    f = open(filepath, "r")

    count = 0
    line = f.readline()

    f_cost = open(output_filepath, "w")

    while line and count < limit:
        count += 1
        if count > limit: break

        trans_type, num = map(int, line.split(","))
        cost = run_transaction(cur, trans_type, num)

        f_cost.write("%s,%s,%s\n" % (trans_type, num, cost))

        line = f.readline()

if __name__ == '__main__':
    if len(sys.argv) not in [4, 5, 6]:
        print "Incorrect arguments"
        sys.exit(1)

    conn = psycopg2.connect(dbname="twitter", host=sys.argv[1],
                            user=sys.argv[2], password=sys.argv[3])
    cur = conn.cursor()

    if len(sys.argv) == 4:
        filepath = os.path.join(os.getcwd(), "input_jobs.txt")
        output_filepath = COST_FILE
    elif len(sys.argv) == 5:
        filepath = sys.argv[4]
        output_filepath = COST_FILE
    else:
        filepath = sys.argv[4]
        output_filepath = sys.argv[5]

    print "READING FROM: %s" % filepath
    print "WRITING TO: %s" % output_filepath

    read_input_file(cur, filepath, output_filepath)
