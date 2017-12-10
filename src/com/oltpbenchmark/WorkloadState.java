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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

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
    
    public WorkloadState(BenchmarkState benchmarkState, List<Phase> works, int num_terminals, TraceReader traceReader) {
        this.benchmarkState = benchmarkState;
        this.works = works;
        this.num_terminals = num_terminals;
        this.workerNeedSleep = num_terminals;
        this.traceReader = traceReader;
        
        phaseIterator = works.iterator();
    }

    //Comparator anonymous class implementation
    public static Comparator<SubmittedProcedure> comp = new Comparator<SubmittedProcedure>(){
        
        @Override
        public int compare(SubmittedProcedure p1, SubmittedProcedure p2) {
	    if (p1.getCost() == 0.0 && p2.getCost() == 0.0) {
		return (int) (p1.getStartTime() - p2.getStartTime());
	    }
	    // Convert cost into some form of deadline, so we can simulate EDF
	    long cost1 = p1.getStartTime() + (long) (p1.getCost() * 75000);
	    long cost2 = p2.getStartTime() + (long) (p2.getCost() * 75000);
	    return (int) (cost1 - cost2);
        }
    };

    private PriorityQueue<SubmittedProcedure> workQueue = new PriorityQueue<SubmittedProcedure>(100, comp);
    
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
                   }
            else if (currentPhase == null || currentPhase.isDisabled()
                || !currentPhase.isRateLimited() || currentPhase.isSerial())
            {
                return;
                   }
            else {
                // Add the specified number of procedures to the end of the queue.
                for (int i = 0; i < amount; ++i) {
		    Object[] proc = currentPhase.chooseTransactionFromFile();

		    int type = (int) proc[0];
		    long startTime = System.nanoTime();
		    int num = (int) proc[1];
		    float cost = (float) proc[2];

		    // Convert cost into some form of deadline, so we can simulate EDF
		    long execTime = (long) (cost * 75000);
		    long deadlineTime = startTime + 2 * execTime;

                    workQueue.add(new SubmittedProcedure(type, startTime,
					    num, cost, execTime, deadlineTime));
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
            && !currentPhase.isRateLimited())
        {
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
	    long currentTime = System.nanoTime();
            while(workQueue.size() > 0) {
		SubmittedProcedure proc = workQueue.peek();
		if (currentTime + proc.getExecTime() > proc.getDeadlineTime()) {
		    // Can not complete this transaction. Just drop it
		    workQueue.poll();
		    droppedTransactions++;
		} else {
		    break;
		}
	    }

            return workQueue.poll();
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
