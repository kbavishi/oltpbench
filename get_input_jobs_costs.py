#! /usr/bin/env python

import os
import psycopg2
import subprocess
import shlex
import sys

LIMIT_FOLLOWERS = 400
LIMIT_TWEETS_FOR_UID = 10

COST_FILE = os.path.join(os.environ.get("HOME"), "input_jobs_cost.txt")
COST_LOC_FILE = os.path.join(os.environ.get("HOME"), "input_jobs_loc_cost.txt")

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

    return cost, ",".join(uid_list)

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
        return get_tweet(cur, num), None
    elif trans_type == 2:
        return get_tweets_from_following(cur, num)
    elif trans_type == 3:
        return get_followers(cur, num), None
    elif trans_type == 4:
        return get_user_tweets(cur, num), None
    elif trans_type == 5:
        #return insert_tweet(num)
        # XXX - What to do with insertTweet?
        return 100.0, None
    else:
        assert False, "Unknown transaction type: %s" % trans_type

def read_input_file(cur, limit=2000000):
    f = open(os.path.join(os.getenv("HOME"), "input_jobs.txt"), "r")

    count = 0
    line = f.readline()

    f_cost = open(COST_FILE, "w")
    f_loc_cost = open(COST_LOC_FILE, "w")

    while line and count < limit:
        count += 1
        if count > limit: break

        trans_type, num = map(int, line.split(","))
        cost, extra = run_transaction(cur, trans_type, num)

        f_cost.write("%s,%s,%s\n" % (trans_type, num, cost))
        f_loc_cost.write("%s,%s,%s,%s\n" % (trans_type, num, cost, extra))

        line = f.readline()

def create_table_stats_file(cur):
    cur.execute("SELECT relpages, reltuples FROM pg_class WHERE relname = 'tweets'")
    relpages, reltuples = cur.fetchone()

    cur.execute("SELECT n_distinct, most_common_vals, most_common_freqs FROM pg_stats "
                "WHERE tablename='tweets' AND attname='uid'")
    n_distinct, most_common_vals, most_common_freqs = cur.fetchone()
    most_common_vals = most_common_vals.strip("{").strip("}")
    most_common_freqs = ",".join(map(str, most_common_freqs))

    with open(os.path.join(os.getenv("HOME"), "table_stats.txt"), "w") as f:
        f.write("%s,%s\n" % (int(relpages), int(reltuples)))
        f.write("%s\n" % int(n_distinct))
        f.write("%s\n" % most_common_vals)
        f.write("%s" % most_common_freqs)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Incorrect arguments"
        sys.exit(1)

    conn = psycopg2.connect(dbname="twitter", host=sys.argv[1],
                            user=sys.argv[2], password=sys.argv[3])
    cur = conn.cursor()
    create_table_stats_file(cur)
    read_input_file(cur)
