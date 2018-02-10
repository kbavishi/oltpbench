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
import java.util.PriorityQueue;
import java.util.Queue;

import com.oltpbenchmark.types.SchedPolicy;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.QueueLimitException;
import org.apache.log4j.Logger;

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
    private static final float RANDOM_PAGE_COST = (float) 18.5;

    private static final Logger LOG = Logger.getLogger(ThreadBench.class);
    
    private BenchmarkState benchmarkState;
    private int workersWaiting = 0;
    private int workersWorking = 0;
    private int num_terminals;
    private int workerNeedSleep;
    private int droppedTransactions = 0;
    
    private List<Phase> works = new ArrayList<Phase>();
    private Iterator<Phase> phaseIterator;
    private Phase currentPhase = null;
    private long phaseStartNs = 0;
    private TraceReader traceReader = null;

    private LinkedList<LinkedList<Long>> resultsQueue = new LinkedList<LinkedList<Long>>();
    private HashMap<Long, Integer> resultsUnion = new HashMap<Long, Integer>();

    private int schedPolicy;
    private int RESULTS_QUEUE_LIMIT;
    private Queue<SubmittedProcedure> workQueue;
    private HashMap<Integer, Double> costSlope = new HashMap<Integer, Double>();
    private double alpha = 0.5;
    private static double gedfFactor = 0.4;
    private boolean fixedDeadline = false;
    private long defaultDeadlineNs = 500000000;

    private int tweetRelPages;
    private int tweetRelTuples;
    private int tweetRelNDistinct;
    private double tweetsDefaultSelectivity = 0.0;
    private HashMap<Integer, Double> tweetRelFreqMap = new HashMap<Integer, Double>();
    private HashMap<Long, Double> tweetsHitProbMap = new HashMap<Long, Double>();
    private double tweetsDefaultHitProb;
    
    private int followsRelPages;
    private int followsRelTuples;
    private int followsRelNDistinct;
    private double followsDefaultSelectivity = 0.0;
    private HashMap<Integer, Double> followsRelFreqMap = new HashMap<Integer, Double>();
    private double followsDefaultHitProb;

    private int followersRelPages;
    private int followersRelTuples;
    private int followersRelNDistinct;
    private double followersDefaultSelectivity = 0.0;
    private HashMap<Integer, Double> followersRelFreqMap = new HashMap<Integer, Double>();
    private double followersDefaultHitProb;

    private double tweetsUidDefaultHitProb;

    private int usersRelPages;
    private int usersRelTuples;
    private double usersDefaultHitProb;

    public WorkloadState(BenchmarkState benchmarkState, List<Phase> works, int num_terminals,
            int schedPolicy, double alpha, double gedfFactor, int predResultsHistory,
            boolean fixedDeadline, long defaultDeadlineNs, TraceReader traceReader) {
        this.benchmarkState = benchmarkState;
        this.works = works;
        this.num_terminals = num_terminals;
        this.workerNeedSleep = num_terminals;
        this.schedPolicy = schedPolicy;
        this.alpha = alpha;
        this.gedfFactor = gedfFactor;
        this.RESULTS_QUEUE_LIMIT = predResultsHistory;
        this.fixedDeadline = fixedDeadline;
        this.defaultDeadlineNs = defaultDeadlineNs;
        this.traceReader = traceReader;
        
        phaseIterator = works.iterator();
        createWorkQueue();
        switch (SchedPolicy.valueOf(this.schedPolicy)) {
            case EDF_PRED_LOC:
            case GEDF_PRED_LOC:
                try {
                    loadTweetsStatsFile();
                } catch (IOException e) {
                    LOG.info("Unable to load table stats file");
                }
                break;
            case EDF_PRED_BUF_LOC:
            case GEDF_PRED_BUF_LOC:
                try {
                    loadTweetsStatsFile();
                    loadBufStatsFile();
                } catch (IOException e) {
                    LOG.info("Unable to load table / buffer stats file");
                }
                break;
            case EDF_PRED_BUF_LOC_FULL:
            case GEDF_PRED_BUF_LOC_FULL:
                try {
                    loadTweetsStatsFile();
                    loadFollowsStatsFile();
                    loadUsersStatsFile();
                    loadBufStatsFile();
                } catch (IOException e) {
                    LOG.info("Unable to load table / buffer stats file");
                }
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

    private void createWorkQueue() {
        switch (SchedPolicy.valueOf(this.schedPolicy)) {
            case FIFO:
                workQueue = new LinkedList<SubmittedProcedure>();
                break;
            case EDF:
            case EDF_PRED_LOC:
            case EDF_PRED_LOC_OLD:
            case EDF_PRED_BUF_LOC:
            case EDF_PRED_BUF_LOC_FULL:
                workQueue = new PriorityQueue<SubmittedProcedure>(100, edfComp);
                break;
            case GEDF:
            case GEDF_PRED_LOC:
            case GEDF_PRED_LOC_OLD:
            case GEDF_PRED_BUF_LOC:
            case GEDF_PRED_BUF_LOC_FULL:
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
        this.tweetRelPages = Integer.parseInt(array[0]);
        this.tweetRelTuples = Integer.parseInt(array[1]);
        this.tweetRelNDistinct = Integer.parseInt(tableStats.readLine());

        nextLine = tableStats.readLine();
        String[] mc_vals = nextLine.split(",", 0);
        nextLine = tableStats.readLine();
        String[] mc_freqs = nextLine.split(",", 0);

        double tweetsSumFreq = 0.0;
        for (int i=0; i<mc_vals.length; i++) {
            tweetsSumFreq += Double.parseDouble(mc_freqs[i]);
            this.tweetRelFreqMap.put(Integer.parseInt(mc_vals[i]),
                                     Double.parseDouble(mc_freqs[i]));
        }
        this.tweetsDefaultSelectivity = (1 - tweetsSumFreq) /
            (this.tweetRelNDistinct - tweetRelFreqMap.size());
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

        // Ignore the first 4 partition lines
        String nextLine = bufferStats.readLine();
        this.followsDefaultHitProb = Double.parseDouble(nextLine);

        nextLine = bufferStats.readLine();
        this.followersDefaultHitProb = Double.parseDouble(nextLine);

        nextLine = bufferStats.readLine();
        this.tweetsUidDefaultHitProb = Double.parseDouble(nextLine);

        nextLine = bufferStats.readLine();
        this.usersDefaultHitProb = Double.parseDouble(nextLine);

        // Now start the partitions of tweets by the post popular users
        nextLine = bufferStats.readLine();
        Long pred_uid = 1L;
        while (nextLine != null) {
            double hit_prob = Double.parseDouble(nextLine);
            this.tweetsHitProbMap.put(pred_uid, hit_prob);
            nextLine = bufferStats.readLine();
            pred_uid++;
        }

        // We reached the end. We need to remove the last entry and use that as
        // default hit prob
        this.tweetsDefaultHitProb = this.tweetsHitProbMap.remove(pred_uid-1);
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
            }
    
            assert amount > 0;
    
            // Only use the work queue if the phase is enabled and rate limited.
            if (traceReader != null && currentPhase != null) {
                if (benchmarkState.getState() != State.WARMUP) {
                    LinkedList<SubmittedProcedure> list = 
                        traceReader.getProcedures(System.nanoTime());
                    ListIterator it = list.listIterator(0);
                    while (it.hasNext()) {
                        workQueue.add((SubmittedProcedure)it.next());
                    }
               }
           } else if (currentPhase == null || currentPhase.isDisabled()
                || !currentPhase.isRateLimited() || currentPhase.isSerial()) {
                return;
           } else {
                if (benchmarkState.getState() != State.WARMUP) {
                    boolean fullBufPLA = (SchedPolicy.valueOf(this.schedPolicy) ==
                                          SchedPolicy.EDF_PRED_BUF_LOC_FULL);

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

                        if (fullBufPLA && pred == null) {
                            // For Query type 2, we have to look at the
                            // individual predicates to find out the reduction.
                            // For everything else, it is quite simple
                            double hitRate, sel, reduction = 0.0;
                            if (type == 1) {
                                // GetTweet: We assume that the unpopular tweets
                                // partition is touched
                                hitRate = this.tweetsDefaultHitProb;
                                // Just 1 disk I/O
                                sel = 1;
                                reduction = (sel * tweetRelTuples * hitRate * RANDOM_PAGE_COST);
                            } else if (type == 3) {
                                // GetFollowers info
                                // First, we find the followers
                                hitRate = this.followersDefaultHitProb;
                                sel = followersRelFreqMap.getOrDefault(num,
                                        followersDefaultSelectivity);
                                reduction += (Math.min(100, sel * followersRelTuples) *
                                              hitRate * RANDOM_PAGE_COST);
                                // Next, we fetch user profile info about those
                                // followers
                                hitRate = this.usersDefaultHitProb;
                                reduction += (Math.min(100, sel * followersRelTuples) *
                                              hitRate * RANDOM_PAGE_COST);
                            } else if (type == 4) {
                                // GetTweetsForUser
                                hitRate = this.tweetsHitProbMap.getOrDefault(num,
                                        tweetsDefaultHitProb);
                                sel = followersRelFreqMap.getOrDefault(num,
                                        followersDefaultSelectivity);
                                reduction += (Math.min(10, sel * followersRelTuples) *
                                              hitRate * RANDOM_PAGE_COST);
                            } else if (type == 5) {
                                // InsertTweet
                                // No reduction in disk I/Os
                            }
                            cost -= reduction;

                        } else if (pred != null && RESULTS_QUEUE_LIMIT != 0) {
                            // Check if we ran some query in the past with similar predicates
                            Iterator it = pred.iterator();
                            double reduction = 0.0;
                            HashMap<Long, Boolean> map;

                            boolean oldPLA = ((SchedPolicy.valueOf(this.schedPolicy) ==
                                               SchedPolicy.EDF_PRED_LOC_OLD) ||
                                              (SchedPolicy.valueOf(this.schedPolicy) ==
                                               SchedPolicy.GEDF_PRED_LOC_OLD));

                            // Iterate over all the predicate values
                            while (it.hasNext()) {
                                Long predUid = (Long) it.next();

                                int count = resultsUnion.getOrDefault(predUid, 0);
                                if (count != 0) {
                                    double hitRate = count * 1.0 / resultsQueue.size();
                                    double sel = tweetRelFreqMap.getOrDefault(predUid,
                                            tweetsDefaultSelectivity);
                                    if (oldPLA) {
                                        // Reduction is roughly 599.95 per
                                        // predicate. We reduce by close to 50%
                                        reduction += 1500.0;
                                    } else {
                                        // We start with an initial frequency
                                        // corresponding to the relative
                                        // popularity of the predicate
                                        reduction += (sel * tweetRelTuples *
                                                      hitRate * RANDOM_PAGE_COST);
                                    }
                                }
                            }
                            cost -= reduction;
                        } else if (pred != null) {
                            // Common for both fullBufPLA and BufPLA
                            Iterator it = pred.iterator();
                            double hitRate, sel, reduction = 0.0;
                            while (it.hasNext()) {
                                Long predUid = (Long) it.next();
                                // We need to use the buffer pool hit
                                // probability estimates
                                hitRate = tweetsHitProbMap.getOrDefault(predUid,
                                        tweetsDefaultHitProb);
                                sel = tweetRelFreqMap.getOrDefault(predUid,
                                        tweetsDefaultSelectivity);
                                reduction += (sel * tweetRelTuples *
                                              hitRate * RANDOM_PAGE_COST);
                            }

                            if (fullBufPLA) {
                                // Also need to discount for the initial
                                // checking of followers table
                                hitRate = this.followsDefaultHitProb;
                                sel = followsRelFreqMap.getOrDefault(num,
                                        followsDefaultSelectivity);
                                reduction += (Math.min(100, sel * followsRelTuples) *
                                              hitRate * RANDOM_PAGE_COST);
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

                        workQueue.add(new SubmittedProcedure(type, startTime,
                                num, cost, execTime, deadlineTime));
                    }
                }
            }

            // Can't keep up with current rate? Remove the oldest transactions
            // (from the front of the queue).
            while(workQueue.size() > RATE_QUEUE_LIMIT) {
                workQueue.poll();
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
                        workQueue.poll();
                        droppedTransactions++;
                    } else {
                        break;
                    }
                }
            }
            return workQueue.poll();
        }
    }

    public void updateTweetResults(ArrayList<Object> results) {
        if (RESULTS_QUEUE_LIMIT == 0) {
            // We don't need to store results. Return
            return;
        }
        Iterator it = results.iterator();
        LinkedList<Long> predicates = new LinkedList<Long>();

        synchronized (this) {
            while (it.hasNext()) {
                Long nextElem = (Long) it.next();
                predicates.add(nextElem);
                Integer prevValue = resultsUnion.get(nextElem);
                resultsUnion.put(nextElem, prevValue == null ? 1 : prevValue + 1);
            }
            resultsQueue.add(predicates);

            // Need to remove older result predicates
            while (resultsQueue.size() > RESULTS_QUEUE_LIMIT) {
                predicates = resultsQueue.remove();
                it = predicates.iterator();

                // Also update results union by decrementing or removing
                // predicate counters
                while (it.hasNext()) {
                    Long nextElem = (Long) it.next();
                    Integer prevValue = resultsUnion.get(nextElem);
                    if (prevValue == 1) {
                        resultsUnion.remove(nextElem);
                    } else {
                        resultsUnion.put(nextElem, prevValue - 1);
                    }
                }
            }
        }
    }

    public int getDroppedTransactions() {
        return droppedTransactions;
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
