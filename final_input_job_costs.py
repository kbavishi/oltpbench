#! /usr/bin/env python

import os
import psycopg2
import subprocess
import shlex
import sys
from collections import namedtuple

LIMIT_FOLLOWERS = 400
LIMIT_TWEETS_FOR_UID = 10
TF_FACTOR = 4

# In terms of microseconds. Raw costs returned by postgres_tune.py
#Cs, Cr, Ct, Ci, Co = 1.0, 18.73, 0.0017, 0.0014, 0.0002
Cs, Cr, Ct, Ci, Co = 1.0, 60.6847, 0.0032, 0.0035, 0.0004

stats = {}
Stats = namedtuple("Stats", ["relpages", "reltuples", "n_distinct",
                             "most_common_freqs", "sum_mcf", "tree_level",
                             "tuples_per_page"])

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
    # Sorted index scan
    #cost = (fstats.tree_level + 1 + efr / fstats.tuples_per_page) * Cr + efr * (Ci + Ct)
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

        # Unclustered index scan
        cost += (tstats.tree_level + 1 + etr) * Cr + (etr) * (Ci+Ct)
        # Clustered index scan
        #cost += (tstats.tree_level + 1 + etr / tstats.tuples_per_page) * Cr + (etr) * (Ci+Ct)

    return cost, ",".join(map(str, follow_uid_list))

def get_followers(cur, uid):
    fstats = stats["followers"]
    ustats = stats["user_profiles"]

    if uid in fstats.most_common_freqs:
        efr = fstats.reltuples * fstats.most_common_freqs[uid]
    else:
        sel = (1 - fstats.sum_mcf) / fstats.n_distinct
        efr = fstats.reltuples * sel

    efr = min(efr, LIMIT_FOLLOWERS)
    # Sorted index scan
    cost = (fstats.tree_level + 1 + efr / fstats.tuples_per_page) * Cr + efr * (Ci + Ct)

    eur = efr
    # Sorted index scan
    cost += (ustats.tree_level + 1 + eur / ustats.tuples_per_page) * Cr + eur * (Ci + Ct)

    return cost

def get_user_tweets(cur, uid):
    tstats = stats["tweets"]

    if uid in tstats.most_common_freqs:
        etr = tstats.reltuples * tstats.most_common_freqs[uid]
    else:
        sel = (1 - tstats.sum_mcf) / tstats.n_distinct
        etr = tstats.reltuples * sel

    etr = min(etr, LIMIT_TWEETS_FOR_UID)
    # Sorted index scan
    cost = (tstats.tree_level + 1 + etr/tstats.tuples_per_page) * Cr + etr * (Ci + Ct)
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
        return insert_tweet(cur, num) * 10.0, None
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
        cost, extra = run_transaction(cur, trans_type, num)

        if extra:
            f_cost.write("%s,%s,%.2f,%s\n" % (trans_type, num, cost, extra))
        else:
            f_cost.write("%s,%s,%.2f\n" % (trans_type, num, cost))

        line = f.readline()

def create_tweets_stats_file(cur):
    cur.execute("SELECT relpages FROM pg_class WHERE relname = 'tweets'")
    relpages = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT(uid)) FROM tweets")
    n_distinct = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM tweets")
    reltuples = cur.fetchone()[0]
    avg_tweets = reltuples * 1.0 / n_distinct

    cur.execute("SELECT table_len, tuple_count FROM pgstattuple('tweets')")
    table_len, tuple_count = cur.fetchone()
    tuples_per_page = table_len / tuple_count

    cur.execute("SELECT uid, COUNT(*) FROM tweets "
                "GROUP BY uid HAVING COUNT(*) > %s" % int(TF_FACTOR*avg_tweets))
    results = cur.fetchall()

    # Update in-memory information
    mcf = {}
    sum_mcf = 0.0
    for uid, count in results:
        freq = count * 1.0 / reltuples
        mcf[uid] = freq
        sum_mcf += freq

    most_common_vals, most_common_freqs = [], []
    for uid, freq in mcf.items():
        most_common_vals += [uid]
        most_common_freqs += [freq]

    most_common_vals = ",".join(map(str, most_common_vals))
    most_common_freqs = ",".join(map(str, most_common_freqs))

    cur.execute("SELECT tree_level FROM pgstatindex('idx_tweets_uid')")
    tree_level = int(cur.fetchone()[0])

    # Also update info related to popular predicates
    filepath = os.path.join(os.environ.get("HOME"), "buffer_stats.txt")
    lines = open(filepath, "r").readlines()
    # Ignore last line about unpopular partitions
    lines = lines[:-1]
    # Ignore first four lines about other unrelated partitions
    lines = lines[4:]
    for line in lines:
        uid, size = map(int, line.split()[:2])
        freq = size * 1.0 / reltuples
        mcf[uid] = freq
        sum_mcf += freq

    stats["tweets"] = Stats(relpages, reltuples, n_distinct,
                            mcf, sum_mcf, tree_level, tuples_per_page)
    filename = os.path.join(os.getenv("HOME"), "tweets_stats.txt")
    with open(filename, "w") as f:
        f.write("%s,%s\n" % (int(relpages), int(reltuples)))
        f.write("%s\n" % int(n_distinct))
        f.write("%s\n" % int(tree_level))
        f.write("%s\n" % int(tuples_per_page))
        f.write("%s\n" % most_common_vals)
        f.write("%s" % most_common_freqs)

def create_table_stats_file(cur, table_name, attr_name=None, index_name=None):
    cur.execute("SELECT relpages FROM pg_class WHERE relname = '%s'" % table_name)
    relpages = cur.fetchone()[0]

    n_distinct, reltuples, most_common_vals, most_common_freqs, tuples_per_page = \
        None, 0, None, None, 0
    if attr_name:
        cur.execute("SELECT most_common_vals, most_common_freqs FROM pg_stats "
                    "WHERE tablename='%s' AND attname='%s'" % (table_name, attr_name))
        most_common_vals, most_common_freqs = cur.fetchone()

        cur.execute("SELECT table_len, tuple_count FROM pgstattuple('%s')" %
                    table_name)
        table_len, tuple_count = cur.fetchone()
        tuples_per_page = table_len / tuple_count

    if not most_common_vals:
        most_common_vals = ""
    else:
        most_common_vals = most_common_vals.strip("{").strip("}")

    if table_name == "follows" or table_name == "followers" or \
            table_name == "user_profiles":
        cur.execute("SELECT COUNT(*) FROM user_profiles");
        n_distinct = cur.fetchone()[0]
        reltuples = n_distinct

    tree_level = None
    if index_name:
        cur.execute("SELECT tree_level FROM pgstatindex('%s')" % index_name)
        tree_level = int(cur.fetchone()[0])

    # Update in-memory information
    mcv = map(int, filter(None, most_common_vals.split(",")))
    mcf = {}
    sum_mcf = 0.0
    for i in xrange(len(mcv)):
        mcf[mcv[i]] = most_common_freqs[i]
        sum_mcf += most_common_freqs[i]

    stats[table_name] = Stats(relpages, reltuples, n_distinct,
                              mcf, sum_mcf, tree_level, tuples_per_page)
    if most_common_freqs:
        most_common_freqs = ",".join(map(str, most_common_freqs))

    filename = os.path.join(os.getenv("HOME"), "%s_stats.txt" % table_name)
    with open(filename, "w") as f:
        f.write("%s,%s\n" % (int(relpages), int(reltuples)))
        if n_distinct:
            f.write("%s\n" % int(n_distinct))
        if tree_level:
            f.write("%s\n" % int(tree_level))
        if tuples_per_page:
            f.write("%s\n" % int(tuples_per_page))
        if most_common_vals:
            f.write("%s\n" % most_common_vals)
        if most_common_freqs:
            f.write("%s" % most_common_freqs)

if __name__ == '__main__':
    if len(sys.argv) not in [4, 5, 6]:
        print "Incorrect arguments"
        sys.exit(1)

    conn = psycopg2.connect(dbname="twitter", host=sys.argv[1],
                            user=sys.argv[2], password=sys.argv[3])
    cur = conn.cursor()

    if len(sys.argv) == 4:
        # Create table stats files
        cur.execute("CREATE EXTENSION pgstattuple")
        cur.execute("ANALYZE")
        create_tweets_stats_file(cur)
        create_table_stats_file(cur, "follows", "f1", "follows_pkey")
        create_table_stats_file(cur, "followers", "f1", "followers_pkey")
        create_table_stats_file(cur, "user_profiles", "uid", "user_profiles_pkey")
        create_table_stats_file(cur, "idx_tweets_uid")

        # Input and output files
        filepath = os.path.join(os.getcwd(), "input_jobs.txt")
        output_filepath = os.path.join(os.getcwd(), "input_jobs_loc_cost_new.txt")

    elif len(sys.argv) == 5:
        filepath = sys.argv[4]
        output_filepath = os.path.join(os.getcwd(), "input_jobs_loc_cost_new.txt")
    else:
        filepath = sys.argv[4]
        output_filepath = sys.argv[5]

    print "READING FROM: %s" % filepath
    print "WRITING TO: %s" % output_filepath

    read_input_file(cur, filepath, output_filepath)
