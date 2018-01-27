#! /usr/bin/env python

from scipy.stats.stats import pearsonr

def get_latency_nums(filename):
    lines = open(filename, "r").readlines()
    nums = []
    for line in lines:
        nums += [line.split(",")[3]]
    return nums
        
def get_all_latency_nums(csv_file, iterations=11):
    all_nums = []
    for i in xrange(iterations):
        filename = "results/%s.%d.csv" % (csv_file, i)
        all_nums.extend(get_latency_nums(filename))
    return all_nums

def get_exec_nums(filename):
    lines = open(filename, "r").readlines()
    nums = []
    for line in lines:
        tokens = line.split(",")
        qtype, exec_time, cost, exp_exec_time = \
            tokens[0], tokens[4], tokens[5], tokens[6]
        nums += ["%s,%s,%s,%s" % (qtype, exec_time, cost, exec_time)]

    return nums
        
def get_all_exec_nums(csv_file, iterations=11):
    all_nums = []
    for i in xrange(iterations):
        filename = "results/%s.%d.csv" % (csv_file, i)
        all_nums.extend(get_exec_nums(filename))

    exec_times_by_type = {}
    exp_exec_times_by_type = {}

    for line in all_nums:
        tokens = line.split(",")
        qtype, exec_time, exp_exec_time = \
            int(tokens[0]), int(tokens[1]), int(tokens[3])
        if qtype in exec_times_by_type:
            exec_times_by_type[qtype] += [exec_time]
            exp_exec_times_by_type[qtype] += [exp_exec_time]
        else:
            exec_times_by_type[qtype] = [exec_time]
            exp_exec_times_by_type[qtype] = [exp_exec_time]

    print "-" * 10
    print csv_file
    print "-" * 10
    for qtype in sorted(exec_times_by_type.keys()):
        pnr = pearsonr(exp_exec_times_by_type[qtype], exec_times_by_type[qtype])
        print "Type %s: %.3f, %.3f" % (qtype, pnr[0], pnr[1])
        
    return all_nums

def main():
    fifo_nums = get_all_latency_nums("fifo")
    edf_nums = get_all_latency_nums("edf")
    edf_loc_nums = get_all_latency_nums("edf_loc_old")

    open("fifo_nums.csv", "w").write("\n".join(fifo_nums))
    open("edf_nums.csv", "w").write("\n".join(edf_nums))
    open("edf_loc_old_nums.csv", "w").write("\n".join(edf_loc_nums))

    for hist_size in (10, 100, 500, 1000, 5000):
        edf_loc_nums = get_all_latency_nums("edf_loc_%d" % hist_size)
        open("edf_loc_%d_nums.csv" % hist_size, "w").write("\n".join(edf_loc_nums))

    fifo_nums = get_all_exec_nums("fifo")
    edf_nums = get_all_exec_nums("edf")
    edf_loc_nums = get_all_exec_nums("edf_loc_old")

    open("fifo_exec.csv", "w").write("\n".join(fifo_nums))
    open("edf_exec.csv", "w").write("\n".join(edf_nums))
    open("edf_loc_old_exec.csv", "w").write("\n".join(edf_loc_nums))

    for hist_size in (10, 100, 500, 1000, 5000):
        edf_loc_nums = get_all_exec_nums("edf_loc_%d" % hist_size)
        open("edf_loc_%d_exec.csv" % hist_size, "w").write("\n".join(edf_loc_nums))

if __name__ == '__main__':
    main()


