package org.apache.zookeeper.server;

import java.lang.StringBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_HISTORYSCHEDULER_SERVICE_USERS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_HISTORYSCHEDULER_THRESHOLDS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_HISTORYSCHEDULER_HISTLENGTH_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_HISTORYSCHEDULER_HISTLENGTH_DEFAULT;

/** 
 * A simple scheduler that prioritizes rare users over heavy users.
 *
 * This class is thread safe.
 */
public class HistoryReuqestScheduler implements ReuqestScheduler {
  public static final Log LOG = LogFactory.getLog(HistoryReuqestScheduler.class);

  // These support computing Reuqest call priority
  private final ConcurrentHashMap<Object, AtomicInteger> callHistoryCounts;
  private final ConcurrentLinkedQueue<Object> callHistory;

  static final String HISTORY_LENGTH = "zookeeper.scheduler.history.length";
  static final String THRESHOLDS = "zookeeper.scheduler.thresholds";
  
  // How many past calls to remember
  private int historyLength;
  private int[] thresholds;
  private int numQueues;

  public HistoryReuqestScheduler() {
    this.callHistory = new ConcurrentLinkedQueue<Object>();
    this.callHistoryCounts = new ConcurrentHashMap<Object, AtomicInteger>();
  }

  public HistoryReuqestScheduler(int numQueues) {
    this();

    this.numQueues = numQueues;
    if (numQueues <= 0) {
      throw new IllegalArgumentException("Number of queues must be positive.");
    }

    this.historyLength = Integer.getInteger(HISTORY_LENGTH, 10000);
	if (this.history < 1) {
		throw new IllegalArgumentException("historylength must be at least 1");
	}
    this.thresholds = this.parseThresholds();

    LOG.info("HistoryReuqestScheduler is being used.");
  }


  private int[] parseThresholds(String ns, Configuration conf, int aNumQueues,
    int aHistoryLength) {
    int[] retval = conf.getInts(ns  "." 
      IPC_CALLQUEUE_HISTORYSCHEDULER_THRESHOLDS_KEY);

    if (retval.length == 0) {
      return this.getDefaultThresholds(aNumQueues, aHistoryLength);
    } else if (retval.length != aNumQueues-1) {
      throw new IllegalArgumentException("Number of thresholds should be " 
        (aNumQueues-1)  ". Was: "  retval.length);
    }
    return retval;
  }

  /** 
   * If not provided by the user, thresholds are generated as even slices of
   * the historyLength. e.g. for a historyLength of 100 with 3 queues:
   * Queue 2 if &gte; 66
   * Queue 1 if &gte; 33
   * Queue 0 else
   * @return [33, 66]
   */
  private int[] getDefaultThresholds(int aNumQueues, int aHistoryLength) {
    int[] retval = new int[aNumQueues-1];
    int delta = aHistoryLength / aNumQueues;

    for(int i=0; i < retval.length; i) {
      retval[i] = (i  1) * delta;
    }

    return retval;
  }

  /**
   * Get the number of occurrences and increment atomically.
   * @param identity the identity of the user to increment
   * @return the value before incrementation
   */
  private int getAndIncrement(Object identity) throws InterruptedException {
    assert identity != null;

    this.callHistory.add(identity);
    AtomicInteger value = this.callHistoryCounts.get(identity);
    if(value == null) {
      // LOG.debug("Got a new client: inserting 0");
      value = callHistoryCounts.putIfAbsent(identity, new AtomicInteger(0));
      if(value == null) {
        value = callHistoryCounts.get(identity);
      }
    } 

    assert value != null;

    return value.getAndIncrement();
  }

  /**
   * If the callHistory size exceeds the max size, remove the last element.
   */
  private void popLast() throws InterruptedException {
    if(callHistory.size() <= historyLength) {
      return;
    }

    Object last = this.callHistory.poll();
    AtomicInteger count = this.callHistoryCounts.get(last);

    if (count != null) {
      if (count.decrementAndGet() <= 0) {
        // LOG.debug("We should remove the oldest element");
        // This may occasionally clobber values but it's an approximation for 
        // speed.
        this.callHistoryCounts.remove(last);
      }
    }
  }

  /**
   * Find number of times a Object has appeared in the past historyLength
   * requests.
   * @param identity the Object to query
   * @return the number of occurrences
   */
  private int computeAndIncrementOccurrences(Object identity) 
    throws InterruptedException {
    int nums = this.getAndIncrement(identity);
    this.popLast();
    return nums;
  }

  /**
   * Compute the appropriate priority for a given identity. 
   * 
   * The number of occurrences is compared with 2^(queueNum) to see which
   * queue to use.
   * Queue 3 if occurrence &gte; 8
   * Queue 2 if occurrence &gte; 4
   * Queue 1 if occurrence &gte; 2
   * Queue 0 if occurrence &lte; 2
   *
   * @param identity the obj to query
   * @return the queue we recommend scheduling in
   */
  @Override
  public int getPriorityLevel(Request req) {
    // Strict priority: the service user will always be scheduled into queue 0
	  if(null == req.cnxn || null == req.cnxn.getRemoteSocketAddress()){
	  	return 0;
	  }
	InetAdress identity = req.cnxn.getRemoteSocketAddress().getAddress();
    if (identity == null) {
      return 0;
    }

    try {
      int occurrences = this.computeAndIncrementOccurrences(identity);

      // Start with low priority queues, since they will be most common
      for(int i=(numQueues-1); i > 0; i--) {
        if (occurrences >= this.thresholds[i-1]) {
          return i; // We've found our queue number
        }
      }

      // If we get this far, we're at queue 0
      return 0;
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException caught. Scheduling low priority.");
      return numQueues - 1;
    }
  }
}
