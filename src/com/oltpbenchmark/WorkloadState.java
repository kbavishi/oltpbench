/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Comparator;
import java.util.List;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.LinkedList;
//import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeSet;
import java.util.NoSuchElementException;

import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;
import com.oltpbenchmark.types.SchedPolicy;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.QueueLimitException;
import com.oltpbenchmark.util.PriorityQueue;
import org.apache.log4j.Logger;

class PredScore {
    public int counter;
    public long element;

    public PredScore(int counter, long element) {
        this.counter = counter;
        this.element = element;
    }
}

/**
 * This class is used to share a state among the workers of a single
 * workload. Worker use it to ask for work and as interface to the global
 * BenchmarkState
 * @author alendit
 *
 */
public class WorkloadState {
    private static final int RATE_QUEUE_LIMIT = 10000;
    // Choose a random page cost slightly lower than the Postgres value

    private static final Logger LOG = Logger.getLogger(ThreadBench.class);
    
    private BenchmarkState benchmarkState;
    private int workersWaiting = 0;
    private int workersWorking = 0;
    private int num_terminals;
    private int workerNeedSleep;
    private int droppedTransactions = 0;
    private long droppedTransactionUsecs = 0;
    
    private List<Phase> works = new ArrayList<Phase>();
    private Iterator<Phase> phaseIterator;
    private Phase currentPhase = null;
    private long phaseStartNs = 0;
    private TraceReader traceReader = null;

    private LinkedList<LinkedList<Long>> resultsQueue = new LinkedList<LinkedList<Long>>();
    private HashMap<Long, Integer> resultsUnion = new HashMap<Long, Integer>();

    private int schedPolicy;
    private int RESULTS_QUEUE_LIMIT;
    private double RANDOM_PAGE_COST = 4.0;
    private Queue<SubmittedProcedure> workQueue;
    private Queue<SubmittedProcedure> ageQueue = new PriorityQueue<SubmittedProcedure>(2*RATE_QUEUE_LIMIT, ageComp);
    private HashMap<Integer, Double> costSlope = new HashMap<Integer, Double>();
    private double alpha = 0.5;
    private static double gedfFactor = 0.4;
    private boolean fixedDeadline = false;
    private long defaultDeadlineNs = 500000000;

    private int tweetsRelPages;
    private int tweetsRelTuples;
    private int tweetsRelNDistinct;
    private int tweetsRelTreeLevel;
    private int tweetsRelTuplesPerPage;
    private double tweetsDefaultSelectivity = 0.0;
    private HashMap<Long, Double> tweetsRelFreqMap = new HashMap<Long, Double>();
    private HashMap<Long, Double> tweetsHitProbMap = new HashMap<Long, Double>();
    private double tweetsDefaultHitProb;
    
    private int followsRelPages;
    private int followsRelTuples;
    private int followsRelNDistinct;
    private int followsRelTreeLevel;
    private int followsRelTuplesPerPage;
    private double followsDefaultSelectivity = 0.0;
    private HashMap<Integer, Double> followsRelFreqMap = new HashMap<Integer, Double>();
    private double followsDefaultHitProb;

    private int followersRelPages;
    private int followersRelTuples;
    private int followersRelNDistinct;
    private int followersRelTreeLevel;
    private int followersRelTuplesPerPage;
    private double followersDefaultSelectivity = 0.0;
    private HashMap<Integer, Double> followersRelFreqMap = new HashMap<Integer, Double>();
    private double followersDefaultHitProb;

    private int tweetsUidRelPages;
    private double tweetsUidDefaultHitProb;

    private int usersRelPages;
    private int usersRelTuples;
    private int usersRelTreeLevel;
    private int usersRelTuplesPerPage;
    private double usersDefaultHitProb;

    private TreeSet<PredScore> bins;
    private int NUM_BINS = 200;
    private HashMap<Long, PredScore> binMap = new HashMap<Long, PredScore>();
    private int BUFFER_SIZE = 750 * 1024 * 1024 / 8192;
    private int Lp = 5;
    private int BIN_WINDOW_THRESHOLD = 7500;
    private int binWindowSize = 0;
    private int unpopularPredicates = 0;

    private boolean printed = false;

    public WorkloadState(BenchmarkState benchmarkState, List<Phase> works, int num_terminals,
            int schedPolicy, double alpha,
            double gedfFactor, int predResultsHistory,
            double randomPageCost, boolean fixedDeadline,
            long defaultDeadlineNs, int numBins, int bufferSize, int binWindowThreshold,
            TraceReader traceReader) {
        this.benchmarkState = benchmarkState;
        this.works = works;
        this.num_terminals = num_terminals;
        this.workerNeedSleep = num_terminals;
        this.schedPolicy = schedPolicy;
        this.alpha = alpha;
        this.gedfFactor = gedfFactor;

        this.RESULTS_QUEUE_LIMIT = predResultsHistory;
        this.RANDOM_PAGE_COST = randomPageCost;
        this.fixedDeadline = fixedDeadline;
        this.defaultDeadlineNs = defaultDeadlineNs;

        this.NUM_BINS = numBins;
        this.BUFFER_SIZE = bufferSize;
        this.BIN_WINDOW_THRESHOLD = binWindowThreshold;

        this.traceReader = traceReader;
        
        phaseIterator = works.iterator();
        createWorkQueue();
        switch (SchedPolicy.valueOf(this.schedPolicy)) {
            case EDF_PRED_BUF_LOC_FULL:
            case GEDF_PRED_BUF_LOC_FULL:
                try {
                    loadTweetsStatsFile();
                    loadFollowsStatsFile();
                    loadFollowersStatsFile();
                    loadUsersStatsFile();
                    loadTweetsUidStatsFile();
                    loadBufStatsFile();
                } catch (IOException e) {
                    LOG.info("Unable to load table / buffer stats file");
                }
                break;
            case EDF_PRED_DYNAMIC:
            case GEDF_PRED_DYNAMIC:
                try {
                    loadTweetsStatsFile();
                    loadFollowsStatsFile();
                    loadFollowersStatsFile();
                    loadUsersStatsFile();
                    loadTweetsUidStatsFile();
                    loadBufStatsFile();
                    resetMisraGries();
                } catch (IOException e) {
                    LOG.info("Unable to load table / buffer stats file");
                }
                break;
        }
        switch (SchedPolicy.valueOf(this.schedPolicy)) {
            case EDF:
            case GEDF:
                costSlope.put(1, 250335.08);
                costSlope.put(2, 106.07);
                costSlope.put(3, 6989.75);
                costSlope.put(4, 271463.75);
                costSlope.put(5, 13819.80);
                break;
            case EDF_PRED_BUF_LOC_FULL:
            case GEDF_PRED_BUF_LOC_FULL:
            case EDF_PRED_DYNAMIC:
            case GEDF_PRED_DYNAMIC:
                costSlope.put(1, 110524.0);
                costSlope.put(2, 74.14);
                costSlope.put(3, 12666.24);
                costSlope.put(4, 206197.40);
                costSlope.put(5, 277487.85);
                break;
        }
    }

    // EDF Comparator anonymous class implementation
    public static Comparator<SubmittedProcedure> edfComp = new Comparator<SubmittedProcedure>(){
        @Override
        public int compare(SubmittedProcedure p1, SubmittedProcedure p2) {
			if (Double.compare(p1.getCost(), 0.0) == 0 &&
                    Double.compare(p2.getCost(), 0.0) == 0) {
				return Long.compare(p1.getStartTime(), p2.getStartTime());
			} else {
				return Long.compare(p1.getDeadlineTime(), p2.getDeadlineTime());
			}
        }
    };

    // GEDF Comparator anonymous class implementation
    public static Comparator<SubmittedProcedure> gedfComp = new Comparator<SubmittedProcedure>(){
        @Override
        public int compare(SubmittedProcedure p1, SubmittedProcedure p2) {
            double p1_cost = p1.getCost();
            double p2_cost = p2.getCost();
            long p1_deadline = p1.getDeadlineTime();
            long p2_deadline = p2.getDeadlineTime();
            long p1_exec_time = p1.getExecTime();
            long p2_exec_time = p2.getExecTime();

			if (Double.compare(p1_cost, 0.0) == 0 && Double.compare(p2_cost, 0.0) == 0) {
                // Both costs are zero. Just imitate FIFO
                return Long.compare(p1.getStartTime(), p2.getStartTime());

            } else if ((1-gedfFactor) * p2_deadline <= p1_deadline &&
                    p1_deadline <= (1+gedfFactor) * p2_deadline) {
                // Equal deadline group. So same group in gEDF. SJF within group
                if (p1_exec_time == p2_exec_time) {
                    return Long.compare(p1.getStartTime(), p2.getStartTime());
                } else {
                    return Long.compare(p1_exec_time, p2_exec_time);
                }
            } else {
                return Long.compare(p1_deadline, p2_deadline);
            }
        }
    };

    // EDF Comparator anonymous class implementation
    public static Comparator<PredScore> binComp = new Comparator<PredScore>(){
        @Override
        public int compare(PredScore p1, PredScore p2) {
            if (p1.counter != p2.counter) {
                return Integer.compare(p1.counter, p2.counter);
            } else {
                return Long.compare(p1.element, p2.element);
            }
        }
    };

    // EDF Comparator anonymous class implementation
    public static Comparator<SubmittedProcedure> ageComp = new Comparator<SubmittedProcedure>(){
        @Override
        public int compare(SubmittedProcedure p1, SubmittedProcedure p2) {
            return Long.compare(p1.getStartTime(), p2.getStartTime());
        }
    };

    private void createWorkQueue() {
        switch (SchedPolicy.valueOf(this.schedPolicy)) {
            case FIFO:
                workQueue = new LinkedList<SubmittedProcedure>();
                break;
            case EDF:
            case EDF_PRED_BUF_LOC_FULL:
            case EDF_PRED_DYNAMIC:
                workQueue = new PriorityQueue<SubmittedProcedure>(100, edfComp);
                break;
            case GEDF:
            case GEDF_PRED_BUF_LOC_FULL:
            case GEDF_PRED_DYNAMIC:
                workQueue = new PriorityQueue<SubmittedProcedure>(100, gedfComp);
                break;

        }
    }

    private void loadTweetsStatsFile() throws IOException {
        String statsFilePath = System.getProperty("user.home") + File.separator + "tweets_stats.txt";
        BufferedReader tableStats;
        try {
            tableStats = new BufferedReader(new FileReader(statsFilePath));
        } catch (FileNotFoundException e) {
            LOG.info("Could not load tweets stats file: " + statsFilePath);
            return;
        }

        String nextLine = tableStats.readLine();
        String[] array = nextLine.split(",", 0);
        this.tweetsRelPages = Integer.parseInt(array[0]);
        this.tweetsRelTuples = Integer.parseInt(array[1]);
        this.tweetsRelNDistinct = Integer.parseInt(tableStats.readLine());
        this.tweetsRelTreeLevel = Integer.parseInt(tableStats.readLine());
        this.tweetsRelTuplesPerPage = Integer.parseInt(tableStats.readLine());

        double tweetsSumFreq = 0.0;
        String[] mc_vals = null;
        String[] mc_freqs = null;
        nextLine = tableStats.readLine();
        if (nextLine != null && !nextLine.equals("")) {
            // Relevant info is available
            mc_vals = nextLine.split(",", 0);
            nextLine = tableStats.readLine();
            mc_freqs = nextLine.split(",", 0);
        }

        if (mc_vals != null && mc_freqs != null) {
            for (int i=0; i<mc_vals.length; i++) {
                tweetsSumFreq += Double.parseDouble(mc_freqs[i]);
                this.tweetsRelFreqMap.put(Long.parseLong(mc_vals[i]),
                                          Double.parseDouble(mc_freqs[i]));
            }
        }
        this.tweetsDefaultSelectivity = (1 - tweetsSumFreq) /
            (this.tweetsRelNDistinct - this.tweetsRelFreqMap.size());
    }

    private void loadFollowsStatsFile() throws IOException {
        String statsFilePath = System.getProperty("user.home") + File.separator + "follows_stats.txt";
        BufferedReader tableStats;
        try {
            tableStats = new BufferedReader(new FileReader(statsFilePath));
        } catch (FileNotFoundException e) {
            LOG.info("Could not load follows stats file: " + statsFilePath);
            return;
        }

        String nextLine = tableStats.readLine();
        String[] array = nextLine.split(",", 0);
        this.followsRelPages = Integer.parseInt(array[0]);
        this.followsRelTuples = Integer.parseInt(array[1]);
        this.followsRelNDistinct = Integer.parseInt(tableStats.readLine());
        this.followsRelTreeLevel = Integer.parseInt(tableStats.readLine());
        this.followsRelTuplesPerPage = Integer.parseInt(tableStats.readLine());

        nextLine = tableStats.readLine();
        String[] mc_vals = nextLine.split(",", 0);
        nextLine = tableStats.readLine();
        String[] mc_freqs = nextLine.split(",", 0);

        double followsSumFreq = 0.0;
        for (int i=0; i<mc_vals.length; i++) {
            followsSumFreq += Double.parseDouble(mc_freqs[i]);
            this.followsRelFreqMap.put(Integer.parseInt(mc_vals[i]),
                                       Double.parseDouble(mc_freqs[i]));
        }
        this.followsDefaultSelectivity = (1 - followsSumFreq) /
            (this.followsRelNDistinct - followsRelFreqMap.size());
    }

    private void loadFollowersStatsFile() throws IOException {
        String statsFilePath = System.getProperty("user.home") + File.separator + "followers_stats.txt";
        BufferedReader tableStats;
        try {
            tableStats = new BufferedReader(new FileReader(statsFilePath));
        } catch (FileNotFoundException e) {
            LOG.info("Could not load followers stats file: " + statsFilePath);
            return;
        }

        String nextLine = tableStats.readLine();
        String[] array = nextLine.split(",", 0);
        this.followersRelPages = Integer.parseInt(array[0]);
        this.followersRelTuples = Integer.parseInt(array[1]);
        this.followersRelNDistinct = Integer.parseInt(tableStats.readLine());
        this.followersRelTreeLevel = Integer.parseInt(tableStats.readLine());
        this.followersRelTuplesPerPage = Integer.parseInt(tableStats.readLine());

        nextLine = tableStats.readLine();
        String[] mc_vals = nextLine.split(",", 0);
        nextLine = tableStats.readLine();
        String[] mc_freqs = nextLine.split(",", 0);

        double followersSumFreq = 0.0;
        for (int i=0; i<mc_vals.length; i++) {
            followersSumFreq += Double.parseDouble(mc_freqs[i]);
            this.followersRelFreqMap.put(Integer.parseInt(mc_vals[i]),
                                         Double.parseDouble(mc_freqs[i]));
        }
        this.followersDefaultSelectivity = (1 - followersSumFreq) /
            (this.followersRelNDistinct - followersRelFreqMap.size());
    }

    private void loadUsersStatsFile() throws IOException {
        String statsFilePath = System.getProperty("user.home") + File.separator + "user_profiles_stats.txt";
        BufferedReader tableStats;
        try {
            tableStats = new BufferedReader(new FileReader(statsFilePath));
        } catch (FileNotFoundException e) {
            LOG.info("Could not load users stats file: " + statsFilePath);
            return;
        }

        String nextLine = tableStats.readLine();
        String[] array = nextLine.split(",", 0);
        this.usersRelPages = Integer.parseInt(array[0]);
        this.usersRelTuples = Integer.parseInt(array[1]);
        tableStats.readLine();
        this.usersRelTreeLevel = Integer.parseInt(tableStats.readLine());
        this.usersRelTuplesPerPage = Integer.parseInt(tableStats.readLine());
    }
    
    private void loadTweetsUidStatsFile() throws IOException {
        String statsFilePath = System.getProperty("user.home") + File.separator + "idx_tweets_uid_stats.txt";
        BufferedReader tableStats;
        try {
            tableStats = new BufferedReader(new FileReader(statsFilePath));
        } catch (FileNotFoundException e) {
            LOG.info("Could not load followers stats file: " + statsFilePath);
            return;
        }

        String nextLine = tableStats.readLine();
        String[] array = nextLine.split(",", 0);
        this.tweetsUidRelPages = Integer.parseInt(array[0]);
    }

    private void loadBufStatsFile() throws IOException {
        String statsFilePath = System.getProperty("user.home") + File.separator + "buffer_stats.txt";
        BufferedReader bufferStats;
        try {
            bufferStats = new BufferedReader(new FileReader(statsFilePath));
        } catch (FileNotFoundException e) {
            LOG.info("Could not load buffer stats file: " + statsFilePath);
            return;
        }

        // Fetch probabilities for first table partitions
        String nextLine = bufferStats.readLine();
        this.followsDefaultHitProb = Double.parseDouble(nextLine);

        nextLine = bufferStats.readLine();
        this.followersDefaultHitProb = Double.parseDouble(nextLine);

        nextLine = bufferStats.readLine();
        this.usersDefaultHitProb = Double.parseDouble(nextLine);

        nextLine = bufferStats.readLine();
        this.tweetsUidDefaultHitProb = Double.parseDouble(nextLine);

        // Now start the partitions of tweets by the post popular users
        nextLine = bufferStats.readLine();
        long pred_uid = 0;
        double freq = 0.0;
        while (nextLine != null) {
            String[] array = nextLine.split(" ", 3);
            pred_uid = Long.parseLong(array[0]);

            // Update popular tweet set sizes
            int size = Integer.parseInt(array[1]);
            freq =  size * 1.0 / tweetsRelTuples;
            this.tweetsRelFreqMap.put(pred_uid, freq);
            this.tweetsDefaultSelectivity -= freq / this.tweetsRelNDistinct;

            // Update hit probability
            double hit_prob = Double.parseDouble(array[2]);
            this.tweetsHitProbMap.put(pred_uid, hit_prob);

            nextLine = bufferStats.readLine();
        }

        // We reached the end. We need to remove the last entry and use that as
        // default hit prob and default selectivity
        this.tweetsDefaultHitProb = this.tweetsHitProbMap.remove(pred_uid);
        this.tweetsRelFreqMap.remove(pred_uid);
    }

    public int getPolicy() {
        return this.schedPolicy;
    }
    public void resetMisraGries() {
        // Must be called from a synchronized method
        if (this.bins != null) {
            bins.clear();
        } else {
            this.bins = new TreeSet<PredScore>(binComp);
        }
        this.binMap.clear();
        this.binWindowSize = 0;
        // This is used to help calculate unpopular predicates
        this.unpopularPredicates = 0;
    }


    public double get_np_val(double x, double rp_val, double sp_val) {
        return (sp_val *
                (1.0 - 1.0/Math.pow((1 + x*rp_val/sp_val), Lp + 1)));
    }

    public double func(double[] access_probs, int[] partition_sizes, double x) {
        double np_val_sum = 0.0;
        for (int i=0; i<access_probs.length; i++) {
            double np_val = get_np_val(x, access_probs[i], partition_sizes[i]);
            np_val_sum += np_val;
        }
        return np_val_sum - BUFFER_SIZE;
    }

    public double bisect(double[] access_probs, int[] partition_sizes) {
        int maxIter = 100;
        double tol = 8.881784197001252e-16;
        double a = 0.0, b = 10.0 * BUFFER_SIZE;

        for (int i=0; i<maxIter; i++) {
            double c = (a+b)/2;
            double f_c = func(access_probs, partition_sizes, c);
            if (f_c == 0 || (b-a)/2 < tol) {
                return c;
            }
            // New interval
            double f_a = func(access_probs, partition_sizes, a);
            if ((f_c > 0 && f_a > 0) || (f_c < 0 && f_a < 0)) {
                // sign(f(c)) == sign(f(a))
                a = c;
            } else {
                b = c;
            }

        }
        return Double.MIN_VALUE;

    }

    public void calculateHitProbs() {
        LOG.info("calculateHitProbs()");
        int num_bins = binMap.size() + 5;
        long[] preds = new long[num_bins];

        // Get partition access probabilities & partition set sizes
        int idx = 0;
        double[] partition_probs = new double[num_bins];
        int[] partition_sizes = new int[num_bins];
        List<Double> weights = currentPhase.getWeights();
        double weight_norm = weights.get(0) + weights.get(1) + weights.get(2) +
            weights.get(3) + weights.get(4);

        // Add info about known tables
        // 1. Follows table
        partition_probs[idx] = (weights.get(1)/weight_norm);
        partition_sizes[idx] = followsRelPages;
        idx++;

        // 2. Followers table
        partition_probs[idx] = (weights.get(2)/weight_norm);
        partition_sizes[idx] = followersRelPages;
        idx++;

        // 3. User profiles table
        partition_probs[idx] = (weights.get(2)/weight_norm);
        partition_sizes[idx] = usersRelPages;
        idx++;

        // 4. Tweets UID index
        partition_probs[idx] = ((weights.get(1) + weights.get(3))/weight_norm);
        partition_sizes[idx] = tweetsUidRelPages;
        idx++;

        int popular_preds_sizes = 0;
        for (PredScore bin: binMap.values()) {
            LOG.info("Found bin: " + bin.element + ", " + bin.counter);
            // Store predicate value
            preds[idx] = bin.element;

            // Calculate access probability
            partition_probs[idx] = ((weights.get(1) + weights.get(3))/weight_norm) *
                bin.counter * 1.0 / BIN_WINDOW_THRESHOLD;

            // Calculate partition size
            // Assume simple part size calculation
            int size = (int) (tweetsRelFreqMap.getOrDefault(preds[idx],
                            tweetsDefaultSelectivity) * tweetsRelTuples);
            partition_sizes[idx] = size;
            popular_preds_sizes += size;

            idx++;
        }

        int[] new_partition_sizes = new int[num_bins];
        new_partition_sizes[0] = partition_sizes[0];
        new_partition_sizes[1] = partition_sizes[1];
        new_partition_sizes[2] = partition_sizes[2];
        new_partition_sizes[3] = partition_sizes[3];
        new_partition_sizes[num_bins-1] = partition_sizes[num_bins-1];

        popular_preds_sizes = 0;
        for (int i=4; i < num_bins-1; i++) {
            int sum = 0;
            for (int j=4; j < i; j++) {
                sum += partition_sizes[j];
            }
            new_partition_sizes[i] = (int) Math.ceil(partition_sizes[i] * (1.0 - sum * 1.0 / tweetsUidRelPages));
            popular_preds_sizes += new_partition_sizes[i];
        }

        // Add partition for unpopular tweets
        partition_probs[num_bins-1] = (weights.get(1) + weights.get(3)) *
            unpopularPredicates * 1.0 / BIN_WINDOW_THRESHOLD;
        partition_sizes[num_bins-1] = tweetsRelPages - popular_preds_sizes;

        // Clear the previous hit probabilities
        this.tweetsHitProbMap.clear();

        // Recalculate them
        double x_val = bisect(partition_probs, new_partition_sizes);
        for (int i=4; i < num_bins-1; i++) {
            double np_val = get_np_val(x_val, partition_probs[i], new_partition_sizes[i]);
            double hit_prob = np_val / partition_sizes[i];

            double prob = 0.0;
            double prob_own = 1.0;
            prob = (1.0 / tweetsUidRelPages) * np_val;
            for (int j=4; j<i; j++) {
                double other_np_val = get_np_val(x_val, partition_probs[j],
                                                 new_partition_sizes[j]);
                prob_own *= (1.0 - new_partition_sizes[j] * 1.0 / tweetsUidRelPages);
                prob += (1.0 / tweetsUidRelPages) * other_np_val;
            }
            prob += prob_own * hit_prob;
            prob = Math.min(1.0, prob);

            this.tweetsHitProbMap.put(preds[i], prob);
            LOG.info("Calculated for pred " + preds[i] + ": " + prob);
        }
        // We also need to calculate the hit prob of the unpopular tweets
        double def_np_val = get_np_val(x_val, partition_probs[num_bins-1],
                                       new_partition_sizes[num_bins-1]);
        for (int j=4; j<num_bins; j++) {
            double other_np_val = get_np_val(x_val, partition_probs[j],
                                             new_partition_sizes[j]);
            def_np_val += (1.0 / tweetsUidRelPages) * other_np_val;
        }
        this.tweetsDefaultHitProb = def_np_val / partition_sizes[num_bins-1];
        LOG.info("Calculated for default pred: " + this.tweetsDefaultHitProb);

    }

    /**
    * Add a request to do work.
    * 
    * @throws QueueLimitException
    */
   public void addToQueue(int amount, boolean resetQueues) throws QueueLimitException {
       synchronized (this) {
            if (resetQueues) {
                workQueue.clear();
                ageQueue.clear();
            }
    
            assert amount > 0;
            boolean isFIFO = (SchedPolicy.valueOf(this.schedPolicy) == SchedPolicy.FIFO);
    
            // Only use the work queue if the phase is enabled and rate limited.
            if (traceReader != null && currentPhase != null) {
                if (benchmarkState.getState() != State.WARMUP) {
                    LinkedList<SubmittedProcedure> list = 
                        traceReader.getProcedures(System.nanoTime());
                    ListIterator it = list.listIterator(0);
                    while (it.hasNext()) {
                        SubmittedProcedure proc = (SubmittedProcedure) it.next();
                        workQueue.add(proc);
                        if (!isFIFO) {
                            ageQueue.add(proc);
                        }
                    }
               }
           } else if (currentPhase == null || currentPhase.isDisabled()
                || !currentPhase.isRateLimited() || currentPhase.isSerial()) {
                return;
           } else {
                if (benchmarkState.getState() != State.WARMUP) {
                    boolean isPLA = ((SchedPolicy.valueOf(this.schedPolicy) !=
                                      SchedPolicy.FIFO) &&
                                     (SchedPolicy.valueOf(this.schedPolicy) !=
                                      SchedPolicy.EDF) &&
                                     (SchedPolicy.valueOf(this.schedPolicy) !=
                                      SchedPolicy.GEDF));

                    // Add the specified number of procedures to the end of the queue.
                    for (int i = 0; i < amount; ++i) {
                        // Pick transaction to be run from file. It will fallback
                        // to the regular generation method if input file is empty
                        Object[] proc = currentPhase.chooseTransactionFromFile();

                        int type = (int) proc[0];
                        long startTime = System.nanoTime();
                        int num = (int) proc[1];
                        double cost = (double) proc[2];
                        ArrayList<Long> pred = (ArrayList<Long>) proc[3];

                        if (isPLA) {
                            // For Query type 2, we have to look at the
                            // individual predicates to find out the reduction.
                            // For everything else, it is quite simple
                            double hitRate, sel, reduction = 0.0;
                            if (type == 1) {
                                // GetTweet: We assume that the unpopular tweets
                                // partition is touched
                                hitRate = this.tweetsDefaultHitProb;
                                int disk_ios = this.tweetsRelTreeLevel + 2;
                                reduction = (disk_ios * hitRate * RANDOM_PAGE_COST);
                            } else if (type == 2) {
                                // Need to reduce cost based on predicates
                                if (pred != null) {
                                    Iterator it = pred.iterator();
                                    while (it.hasNext()) {
                                        Long predUid = (Long) it.next();
                                        // We need to use the buffer pool hit
                                        // probability estimates
                                        hitRate = tweetsHitProbMap.getOrDefault(predUid,
                                                tweetsDefaultHitProb);
                                        sel = tweetsRelFreqMap.getOrDefault(predUid,
                                                tweetsDefaultSelectivity);
                                        int rows = (int) (sel * tweetsRelTuples);
                                        // Clustered index scan
                                        //int disk_ios = (this.tweetsRelTreeLevel + 1 +
                                        //                (1 + rows / this.tweetsRelTuplesPerPage));
                                        // Unclustered index scan
                                        int disk_ios = (this.tweetsRelTreeLevel + 1 + rows);
                                        reduction += (disk_ios * hitRate * RANDOM_PAGE_COST);
                                    }
                                }

                                // Also need to discount for the initial
                                // checking of follows table
                                hitRate = this.followsDefaultHitProb;
                                sel = followsRelFreqMap.getOrDefault(num,
                                        followsDefaultSelectivity);
                                int rows = Math.min(TwitterConstants.LIMIT_FOLLOWERS,
                                                    (int) (sel * followsRelTuples));
                                int disk_ios = (this.followsRelTreeLevel + 1 +
                                                rows / this.followsRelTuplesPerPage);
                                reduction += (disk_ios * hitRate * RANDOM_PAGE_COST);
                            } else if (type == 3) {
                                // GetFollowers info

                                // First, we find the followers
                                hitRate = this.followersDefaultHitProb;
                                sel = followersRelFreqMap.getOrDefault(num,
                                        followersDefaultSelectivity);
                                int rows = Math.min(TwitterConstants.LIMIT_FOLLOWERS,
                                                    (int) (sel * followersRelTuples));
                                int disk_ios = (this.followersRelTreeLevel + 1 +
                                                rows / this.followersRelTuplesPerPage);
                                reduction += (disk_ios * hitRate * RANDOM_PAGE_COST);

                                // Next, we fetch user profile info about those
                                // followers
                                hitRate = this.usersDefaultHitProb;
                                disk_ios = (this.usersRelTreeLevel + 1 +
                                            rows / this.usersRelTuplesPerPage);
                                reduction += (disk_ios * hitRate * RANDOM_PAGE_COST);
                            } else if (type == 4) {
                                // GetUserTweets
                                hitRate = this.tweetsHitProbMap.getOrDefault(num,
                                        tweetsDefaultHitProb);
                                sel = tweetsRelFreqMap.getOrDefault(num,
                                        tweetsDefaultSelectivity);
                                int rows = Math.min(TwitterConstants.LIMIT_TWEETS_FOR_UID,
                                                    (int) (sel * tweetsRelTuples));
                                int disk_ios = (this.tweetsRelTreeLevel + 1 +
                                                rows / this.tweetsRelTuplesPerPage);
                                reduction += (disk_ios * hitRate * RANDOM_PAGE_COST);
                            } else if (type == 5) {
                                // InsertTweet
                                // No reduction in disk I/Os
                            }
                            if (reduction > cost) {
                                LOG.info("KB Malfunction: " + type + ", " + num + ", " + cost + ", " + reduction);
                            }
                            cost -= reduction;

                        }

                        // Convert cost into some form of deadline, so we can simulate EDF
                        long execTime = (long) (cost * costSlope.getOrDefault(type, 25000.0));
                        long deadlineTime;
                        if (this.fixedDeadline) {
                            // Fixed query deadline specified by user
                            deadlineTime = startTime + defaultDeadlineNs;
                        } else {
                            // Pick deadline based on estimated execution time
                            deadlineTime = startTime + 10 * execTime;
                        }
                        if (deadlineTime < startTime) {
                            LOG.info("KB Malfunction: " + type + ", " + num + ", " + cost + ", " +
                                     costSlope.getOrDefault(type, 25000.0) + ", " + execTime);
                        }

                        SubmittedProcedure subProc = new SubmittedProcedure(type, startTime, num,
                                cost, execTime, deadlineTime);
                        workQueue.add(subProc);
                        if (!isFIFO) {
                            ageQueue.add(subProc);
                        }
                    }
                }
            }

            // Can't keep up with current rate? Remove the oldest transactions
            // (from the front of the queue).
            if (workQueue.size() > RATE_QUEUE_LIMIT) {
                long currentTime = System.nanoTime();
                if (isFIFO) {
                    while(workQueue.size() > RATE_QUEUE_LIMIT) {
                        SubmittedProcedure proc = workQueue.poll();
                        droppedTransactions++;
                        droppedTransactionUsecs += ((currentTime - proc.getStartTime()) / 1000);
                    }
                } else {
                    while(workQueue.size() > RATE_QUEUE_LIMIT) {
                        SubmittedProcedure proc = ageQueue.poll();
                        workQueue.remove(proc);
                        droppedTransactions++;
                        droppedTransactionUsecs += ((currentTime - proc.getStartTime()) / 1000);
                    }
                }
            }

            // Wake up sleeping workers to deal with the new work.
            int numToWake = (amount <= workersWaiting? amount : workersWaiting);
            for (int i = 0; i < numToWake; ++i)
                this.notify();
       }
   }

    public boolean getScriptPhaseComplete() {
        assert (traceReader != null);
        synchronized(this) {
            return traceReader.getPhaseComplete() && workQueue.size() == 0 && workersWorking == 0;
        }
   }
   
   public void signalDone() {
       int current = this.benchmarkState.signalDone();
       if (current == 0) {
           synchronized (this) {
               if (workersWaiting > 0) {
                   this.notifyAll();
               }
           }
       }
   }

   public void updateCostEWMA(int type, long execNs, double cost) {
       double currentCostSlope = execNs * 1.0 / cost;
       synchronized(this) {
           costSlope.put(type, this.alpha * currentCostSlope +
                   (1-this.alpha) * costSlope.getOrDefault(type, 25000.0));
       }
   }
   
   /** Called by ThreadPoolThreads when waiting for work. */
    public SubmittedProcedure fetchWork() {
        synchronized(this) {
            if (currentPhase != null && currentPhase.isSerial()) {
                ++workersWaiting;
                while (getGlobalState() == State.LATENCY_COMPLETE) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                --workersWaiting;

                if (getGlobalState() == State.EXIT || getGlobalState() == State.DONE)
                    return null;

                ++workersWorking;
                return new SubmittedProcedure(currentPhase.chooseTransaction(getGlobalState() == State.COLD_QUERY));
            }
        }

        // Unlimited-rate phases don't use the work queue.
        if (currentPhase != null && traceReader == null
            && !currentPhase.isRateLimited()) {
            synchronized(this) {
                ++workersWorking;
            }
            return new SubmittedProcedure(currentPhase.chooseTransaction(getGlobalState() == State.COLD_QUERY));
        }

        synchronized(this) {
            // Sleep until work is available.
            if (workQueue.peek() == null) {
                workersWaiting += 1;
                while (workQueue.peek() == null) {
                    if (this.benchmarkState.getState() == State.EXIT
                        || this.benchmarkState.getState() == State.DONE)
                        return null;

                   try {
                       this.wait();
                   } catch (InterruptedException e) {
                       throw new RuntimeException(e);
                   }
               }
               workersWaiting -= 1;
           }

            assert workQueue.peek() != null;
            ++workersWorking;

            // Return and remove the topmost piece of work, unless we're in the
            // warmup stage of a script, in which case we shouldn't remove it.
            if (traceReader != null && this.benchmarkState.getState() == State.WARMUP)
                return workQueue.peek();

            // Remove transactions which will not complete within the deadlines
            // NOTE - FIFO does not consider deadlines
            if (SchedPolicy.valueOf(this.schedPolicy) != SchedPolicy.FIFO) {
                long currentTime = System.nanoTime();
                while(workQueue.size() > 1) {
                    SubmittedProcedure proc = workQueue.peek();
                    if (currentTime + proc.getExecTime() > proc.getDeadlineTime()) {
                        // Can not complete this transaction. Just drop it
                        proc = workQueue.poll();
                        ageQueue.remove(proc);
                        droppedTransactions++;
                        droppedTransactionUsecs += ((currentTime - proc.getStartTime()) / 1000);
                    } else {
                        break;
                    }
                }
            }

            if ((SchedPolicy.valueOf(this.schedPolicy) == SchedPolicy.EDF_PRED_DYNAMIC) ||
                (SchedPolicy.valueOf(this.schedPolicy) == SchedPolicy.GEDF_PRED_DYNAMIC)) {
                // We maintain a sliding window of predicate results. Reset bins
                // if we have reached the appropriate size
                if (this.binWindowSize > this.BIN_WINDOW_THRESHOLD) {
                    calculateHitProbs();
                    resetMisraGries();
                }
            }
            if (SchedPolicy.valueOf(this.schedPolicy) == SchedPolicy.FIFO) {
                return workQueue.poll();
            } else {
                SubmittedProcedure proc = workQueue.poll();
                ageQueue.remove(proc);
                return proc;
            }
        }
    }

    public void updateBins(ArrayList<Object> results) {
        Iterator it = results.iterator();

        synchronized (this) {
            boolean foundUnpopularPred = false;
            while (it.hasNext()) {
                Long pred = (Long) it.next();
                if (binMap.containsKey(pred)) {
                    // Increment bin counter
                    PredScore bin = binMap.get(pred);
                    bins.remove(bin);
                    binMap.remove(bin.element);
                    bin.counter++;
                    bins.add(bin);
                    binMap.put(pred, bin);
                } else {
                    if (bins.size() < NUM_BINS) {
                        // Found a bin with counter 0. Map the predicate to this
                        // bin.
                        PredScore lowestBin = new PredScore(1, pred);
                        bins.add(lowestBin);
                        binMap.put(lowestBin.element, lowestBin);
                    } else {
                        // Did not find a relevant bin. Decrement all counters
                        Object[] binElems = bins.toArray();
                        bins.clear();
                        for (Object obj: binElems) {
                            PredScore currBin = (PredScore) obj;
                            if (currBin.counter == 1) {
                                // Bin will be decremented to zero. Just clear
                                // bin
                                bins.remove(currBin);
                                binMap.remove(currBin.element);
                                continue;
                            }
                            bins.remove(currBin);
                            binMap.remove(currBin.element);
                            currBin.counter--;
                            bins.add(currBin);
                            binMap.put(currBin.element, currBin);
                        }
                        foundUnpopularPred = true;
                    }
                }
            }
            this.binWindowSize++;
            // For each result, we increment unpopularPredicates to figure out
            // the access probability of the partition containing unpopular
            // tweets
            if (foundUnpopularPred) {
                this.unpopularPredicates++;
            }
        }
    }
    public void updateTweetResults(ArrayList<Object> results) {
        if ((SchedPolicy.valueOf(this.schedPolicy) ==
                SchedPolicy.EDF_PRED_DYNAMIC) ||
            (SchedPolicy.valueOf(this.schedPolicy) ==
                SchedPolicy.GEDF_PRED_DYNAMIC)) {
            // Just update the Misra-Gries bins
            updateBins(results);
        }
    }

    public int getDroppedTransactions() {
        return droppedTransactions;
    }

    public long getDroppedTransactionUsecs() {
        return droppedTransactionUsecs;
    }

    public void printAlpha() {
        synchronized (this) {
            if (printed) {
                return;
            }
            System.out.println("ALPHA 1: " + costSlope.get(1));
            System.out.println("ALPHA 2: " + costSlope.get(2));
            System.out.println("ALPHA 3: " + costSlope.get(3));
            System.out.println("ALPHA 4: " + costSlope.get(4));
            System.out.println("ALPHA 5: " + costSlope.get(5));
            this.printed = true;
        }
    }
    public void finishedWork() {
        synchronized (this) {
            assert workersWorking > 0;
            --workersWorking;
       }
   }
   
   public Phase getNextPhase() {
       if (phaseIterator.hasNext())
           return phaseIterator.next();
       return null;
   }
   
   public Phase getCurrentPhase() {
       synchronized (benchmarkState){
           return currentPhase;
       }
   }
   
   /*
    * Called by workers to ask if they should stay awake in this phase
    */
   public void stayAwake() {
       synchronized(this) {
            while (workerNeedSleep > 0) {
               workerNeedSleep --;
               try {
                   this.wait();
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           }
       }
   }
   
   public void switchToNextPhase() {
       synchronized(this) {
           this.currentPhase = this.getNextPhase();

            // Clear the work from the previous phase.
            workQueue.clear();
            ageQueue.clear();

            // Determine how many workers need to sleep, then make sure they
            // do.
            if (this.currentPhase == null)
                // Benchmark is over---wake everyone up so they can terminate
                workerNeedSleep = 0;
            else {
                this.currentPhase.resetSerial();
                if (this.currentPhase.isDisabled())
                    // Phase disabled---everyone should sleep
                    workerNeedSleep = this.num_terminals;
                else
                    // Phase running---activate the appropriate # of terminals
                    workerNeedSleep = this.num_terminals
                        - this.currentPhase.getActiveTerminals();

                if (traceReader != null)
                    traceReader.changePhase(this.currentPhase.id, System.nanoTime());
           }


            this.notifyAll();
       }
   }
   
   /**
    * Delegates pre-start blocking to the global state handler
    */
   
   public void blockForStart() {
       benchmarkState.blockForStart();

        // For scripted runs, the first one out the gate should tell the
        // benchmark to skip the warmup phase.
        if (traceReader != null) {
            synchronized(benchmarkState) {
                if (benchmarkState.getState() == State.WARMUP)
                    benchmarkState.startMeasure();
            }
        }
   }
   
   /**
    * Delegates a global state query to the benchmark state handler
    * 
    * @return global state
    */
   public State getGlobalState() {
       return benchmarkState.getState();
   }
   
    public void signalLatencyComplete() {
        assert currentPhase.isSerial();
        benchmarkState.signalLatencyComplete();
    }

    public void startColdQuery() {
        assert currentPhase.isSerial();
        benchmarkState.startColdQuery();
    }

    public void startHotQuery() {
        assert currentPhase.isSerial();
        benchmarkState.startHotQuery();
    }

   public long getTestStartNs() {
       return benchmarkState.getTestStartNs();
   }
   
}
