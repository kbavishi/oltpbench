#! /usr/bin/env python

import scipy.optimize as optimize
import numpy as np
import math
import os
import psycopg2
import subprocess
import shlex
import argparse

# Num of buffer pages. 256 MB / 8 KB pages
B = 256 * 3 * 1024 * 1024 / (8 * 1024)

# Weight assigned to a buffer page in the pool on a hit
# From the Naughton cost model paper, picking 5
Lp = 5

rp_vals, sp_vals = [], []

q1, q2, q3, q4, q5 = 0.0025, 0.91, 0.0025, 0.075, 0.01

def get_num_follows_rows(cur, uid):
    cur.execute("SELECT COUNT(*) FROM followers WHERE f1 = %s;" % uid)
    output = cur.fetchall()
    return int(output[0][0])

def get_num_tweets(cur, uid):
    cur.execute("SELECT COUNT(*) FROM tweets WHERE UID = %s;" % uid)
    output = cur.fetchall()
    return int(output[0][0])

def get_tuples_per_page(cur):
    cur.execute("SELECT table_len, tuple_count FROM pgstattuple('tweets')")
    table_len, tuple_count = cur.fetchone()
    return table_len / tuple_count

def get_partition_access_probs(cur, num_partitions):
    all_probs = []

    # Add table partitions
    # 1. Follows
    all_probs += [q2]

    # 2. Followers
    all_probs += [q3]

    # 3. User profiles
    all_probs += [q3]

    # 4. IDX_Tweets_UID
    all_probs += [(q2+q4)]

    # 5 onwards: Popular UID tweet partitions
    cur.execute("SELECT COUNT(*) FROM user_profiles")
    total_user_count = int(cur.fetchall()[0][0])

    cur.execute("SELECT f1, COUNT(*) FROM followers GROUP BY f1 "
                "ORDER BY COUNT(*) DESC LIMIT %d" % num_partitions)
    results = cur.fetchall()

    uid_list = []
    for uid, num in results:
        all_probs += [(q2+q4) * num * 1.0/total_user_count]
        uid_list += [uid]
        print "Got follows result for %s" % uid

    # Last: Unpopular tweets
    cur.execute("SELECT COUNT(DISTINCT(f2)) FROM followers WHERE f1 NOT IN (%s)" %
                ",".join(map(str, uid_list)))
    other_user_count = int(cur.fetchall()[0][0])

    all_probs += [(q2+q4+q1+q5) * other_user_count/total_user_count]
    uid_list += [42]

    filename = os.path.join(os.getenv("HOME"),
                            "hits_stats_%s.txt" % num_partitions)
    f = open(filename, 'w')
    for i in xrange(len(all_probs)):
        if i >= 4:
            text = "ACC_PROB %d: %s" % (uid_list[i-4], all_probs[i])
        else:
            text = "ACC_PROB %d: %s" % (i+1, all_probs[i])
        print text
        f.write("%s\n" % text)

def get_rel_pages(cur, relname):
    cur.execute("SELECT relpages FROM pg_class WHERE relname = '%s'" % relname)
    return int(cur.fetchall()[0][0])

def get_partition_sizes(cur, num_partitions):
    all_sizes = []

    # Add table partitions
    # 1. Follows
    #all_sizes += [get_rel_pages(cur, "follows") +
    #              get_rel_pages(cur, "follows_pkey")]
    all_sizes += [get_rel_pages(cur, "follows")]

    # 2. Followers
    #all_sizes += [get_rel_pages(cur, "followers") +
    #              get_rel_pages(cur, "followers_pkey")]
    all_sizes += [get_rel_pages(cur, "followers")]

    # 3. User profiles
    #all_sizes += [get_rel_pages(cur, "user_profiles") +
    #              get_rel_pages(cur, "user_profiles_pkey")]
    all_sizes += [get_rel_pages(cur, "user_profiles")]

    # 4. IDX_Tweets_UID
    all_sizes += [get_rel_pages(cur, "idx_tweets_uid")]

    # 5 onwards: Popular UID tweet partitions
    cur.execute("SELECT f1, COUNT(*) FROM followers GROUP BY f1 "
                "ORDER BY COUNT(*) DESC LIMIT %d" % num_partitions)
    results = cur.fetchall()

    tuples_per_page = get_tuples_per_page(cur)

    uid_list = []
    popular_tweets_num = 0
    for uid, _ in results:
        uid_list += [uid]
        num_tweets = get_num_tweets(cur, uid)
        size = int(math.ceil(num_tweets * 1.0 / tuples_per_page))
        all_sizes += [size]
        popular_tweets_num += size
        print "Got tweets result for %s" % uid

    # Last: Unpopular tweets partition
    #total_count = get_rel_pages(cur, "tweets") + \
    #    get_rel_pages(cur, "tweets_pkey")
    total_count = get_rel_pages(cur, "tweets")
    all_sizes += [total_count - popular_tweets_num]
    uid_list += [42]

    filename = os.path.join(os.getenv("HOME"),
                            "hits_stats_%s.txt" % num_partitions)
    f = open(filename, 'a')
    for i in xrange(len(all_sizes)):
        if i >= 4:
            text = "PART_SIZE %d: %s" % (uid_list[i-4], all_sizes[i])
        else:
            text = "PART_SIZE %d: %s" % (i+1, all_sizes[i])
        print text
        f.write("%s\n" % text)

def read_vals(cur, num_partitions):
    filename = os.path.join(os.getenv("HOME"),
                            "hits_stats_%s.txt" % num_partitions)
    if not os.path.exists(filename):
        get_partition_access_probs(cur, num_partitions)
        get_partition_sizes(cur, num_partitions)

    lines = open(filename, "r").readlines()
    total_partitions = 4 + num_partitions + 1

    uid_vals = []
    for i in xrange(4, total_partitions):
        uid_vals += [int(lines[i].split()[1].strip(":"))]

    # First 100 lines will give us the access probs
    rp_vals = []
    for i in xrange(total_partitions):
        rp_vals += [float(lines[i].split()[2].strip(","))]

    # Next 100 lines will give us the partition set sizes
    sp_vals = []
    for i in xrange(total_partitions, total_partitions*2):
        sp_vals += [int(lines[i].split()[2])]

    return uid_vals, rp_vals, sp_vals

def get_np_val(x, rp_val, sp_val):
    np_val = sp_val * (1 - 1 / math.pow((1 + x*rp_val/sp_val), Lp + 1))
    return np_val

def func(x):
    global rp_vals, sp_vals
    np_vals = []
    for i in xrange(len(rp_vals)):
        np_val = get_np_val(x, rp_vals[i], sp_vals[i])
        np_vals += [np_val]

    return sum(np_vals) - B

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run EDF tests')
    parser.add_argument('host', metavar='IP_ADDR', type=str,
                        help='IP address of the Postgres instance')
    parser.add_argument('user', metavar='USER', type=str,
                        help='Username to login to Postgres')
    parser.add_argument('password', metavar='PASSWORD', type=str,
                        help='Password to login to Postgres')
    parser.add_argument('--partitions', type=int, default=200,
                        help='Number of popular partitions')
    parser.add_argument('--memory', type=int, default=768,
                        help='Buffer pool memory in MB')

    args = parser.parse_args()
    conn = psycopg2.connect(dbname="twitter", host=args.host, user=args.user,
                            password=args.password)
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION pgstattuple")

    global rp_vals, sp_vals, B
    uid_vals, rp_vals, sp_vals = read_vals(cur, args.partitions)
    B = args.memory * 1024 * 1024 / (8 * 1024)

    ratio = optimize.bisect(func, 0, (B/0.1))
    print ratio

    f = open(os.path.join(os.getenv("HOME"), "buffer_stats.txt"), "w")
    for i in xrange(len(rp_vals)):
        np_val = get_np_val(ratio, rp_vals[i], sp_vals[i])
        hit_prob = np_val / sp_vals[i]
        print "PARTITION_NP %d: %s" % (i+1, hit_prob)
        if i>=4:
            # For the popular tweets partitions, print the predicate, set size
            # and the probability
            f.write("%s %s %s\n" % (uid_vals[i-4], sp_vals[i], hit_prob))
        else:
            # For the initial table partitions, just print the probability
            f.write("%s\n" % hit_prob)
