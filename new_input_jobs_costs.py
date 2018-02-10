#! /usr/bin/env python

import os
import psycopg2
import subprocess
import shlex
import sys
from collections import namedtuple

LIMIT_FOLLOWERS = 100
LIMIT_TWEETS_FOR_UID = 10

stats = {}
Cs, Cr, Ct, Ci, Co = 1.0, 18.73, 0.0017, 0.0014, 0.0002
Stats = namedtuple("Stats", ["relpages", "reltuples", "n_distinct",
                             "most_common_freqs", "sum_mcf", "tree_level"],
                   verbose=True)

def get_plan_cost(output):
    for line in output.split("\n"):
        line = line.strip()
        if not line.startswith("Total Cost"):
            continue
        cost = float(line.split(":")[1])
        return cost

    assert False, "No cost found"

def get_tweet(cur, tid):
    tstats = stats["tweets"]
    return (tstats.tree_level + 1 + 1) * Cr + 1 * (Ci + Ct)

def get_tweets_from_following(cur, uid):
    fstats = stats["follows"]
    if uid in fstats.most_common_freqs:
        efr = fstats.reltuples * fstats.most_common_freqs[uid]
    else:
        sel = (1 - fstats.sum_mcf) / fstats.n_distinct
        efr = fstats.reltuples * sel

    efr = min(efr, LIMIT_FOLLOWERS)
    # Index scan + multiple results within that page
    cost = (fstats.tree_level + 1 + efr) * Cr + efr * (Ci + Ct)

    tstats = stats["tweets"]
    cur.execute("SELECT f2 FROM follows WHERE f1 = %s LIMIT %s;" %
                (uid, LIMIT_FOLLOWERS))
    follow_uids = cur.fetchall()
    follow_uid_list = map(int, [u[0] for u in follow_uids])

    for f_uid in follow_uid_list:
        if f_uid in tstats.most_common_freqs:
            etr = tstats.reltuples * tstats.most_common_freqs[f_uid]
        else:
            sel = (1 - tstats.sum_mcf) / tstats.n_distinct
            etr = tstats.reltuples * sel

        cost += (tstats.tree_level + 1 + etr) * Cr + (etr) * (Ci+Ct)

    return cost, ",".join(map(str, follow_uid_list))

def get_followers(cur, uid):
    fstats = stats["followers"]
    ustats = stats["user_profiles"]

    if uid in fstats.most_common_freqs:
        efr = fstats.reltuples * fstats.most_common_freqs[uid]
    else:
        sel = (1 - fstats.sum_mcf) / fstats.n_distinct
        efr = fstats.reltuples * sel

    # XXX - Ideally there would have been efr random I/Os, but our buffer
    # locality will really discount this, so assume just 1 I/O
    cost = (fstats.tree_level + 1) * Cr + min(efr, LIMIT_FOLLOWERS) * (Ci + Ct)
    eur = min(LIMIT_FOLLOWERS, efr)
    cost += (ustats.tree_level + 1 + eur) * eur * Cr + eur * (Ci + Ct)

    return cost

def get_user_tweets(cur, uid):
    tstats = stats["tweets"]

    if uid in tstats.most_common_freqs:
        etr = tstats.reltuples * tstats.most_common_freqs[uid]
    else:
        sel = (1 - tstats.sum_mcf) / tstats.n_distinct
        etr = tstats.reltuples * sel

    etr = min(etr, LIMIT_TWEETS_FOR_UID)
    cost = (tstats.tree_level + 1 + etr) * Cr + etr * (Ci + Ct)
    return cost

def insert_tweet(cur, uid):
    tstats = stats["tweets"]
    cost = (tstats.tree_level + 1 + 1) * Cr + 1 * (Ci + Ct)
    return cost

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

def read_input_file(cur, limit=2000000, print_pred=False):
    f = open(os.path.join(os.getenv("HOME"), "input_jobs.txt"), "r")

    count = 0
    line = f.readline()

    while line and count < limit:
        count += 1
        if count > limit: break

        trans_type, num = map(int, line.split(","))
        cost, extra = run_transaction(cur, trans_type, num)

        if extra and print_pred:
            print "%s,%s,%s,%s" % (trans_type, num, cost, extra)
        else:
            print "%s,%s,%s" % (trans_type, num, cost)

        line = f.readline()

def create_table_stats_file(cur, table_name, attr_name, index_name):
    cur.execute("SELECT relpages, reltuples FROM pg_class WHERE relname = 'tweets'")
    relpages, reltuples = cur.fetchone()

    cur.execute("SELECT n_distinct, most_common_vals, most_common_freqs FROM pg_stats "
                "WHERE tablename='%s' AND attname='%s'" % (table_name, attr_name))
    n_distinct, most_common_vals, most_common_freqs = cur.fetchone()
    most_common_vals = most_common_vals.strip("{").strip("}")
    cur.execute("SELECT tree_level FROM pgstatindex('%s')" % index_name)
    tree_level = int(cur.fetchone())

    # Update in-memory information
    mcf = {}
    sum_mcf = 0.0
    for i in xrange(len(most_common_vals)):
        mcf[int(most_common_vals[i])] = most_common_freqs[i]
        sum_mcf += most_common_freqs[i]

    stats[table_name] = Stats(relpages, reltuples, n_distinct,
                              mcf, sum_mcf, tree_level)
    most_common_freqs = ",".join(map(str, most_common_freqs))

    filename = os.path.join(os.getenv("HOME"), "%s_stats.txt" % table_name)
    with open(filename, "w") as f:
        f.write("%s,%s\n" % (int(relpages), int(reltuples)))
        f.write("%s\n" % int(n_distinct))
        f.write("%s\n" % tree_level)
        f.write("%s\n" % most_common_vals)
        f.write("%s" % most_common_freqs)

if __name__ == '__main__':
    if len(sys.argv) not in [4, 5]:
        print "Incorrect arguments"
        sys.exit(1)

    conn = psycopg2.connect(dbname="twitter", host=sys.argv[1],
                            user=sys.argv[2], password=sys.argv[3])
    cur = conn.cursor()
    create_table_stats_file(cur, "tweets", "uid", "idx_tweets_uid")
    create_table_stats_file(cur, "follows", "f1", "follows_pkey")
    create_table_stats_file(cur, "followers", "f1", "followers_pkey")

    print_pred = (sys.argv[4] == "true")
    read_input_file(cur, print_pred=print_pred)
