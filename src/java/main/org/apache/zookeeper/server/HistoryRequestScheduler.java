package org.apache.zookeeper.server;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * A simple scheduler that prioritizes rare users over heavy users.
 *
 * This class is thread safe.
 */
public class HistoryRequestScheduler implements RequestScheduler {
  public static final Logger LOG = LoggerFactory.getLogger(HistoryRequestScheduler.class);

  // These support computing Reuqest call priority
  private final ConcurrentHashMap<Object, AtomicInteger> callHistoryCounts;
  private final ConcurrentLinkedQueue<Object> callHistory;

  static final String HISTORY_LENGTH = "zookeeper.scheduler.history.length";
  static final String THRESHOLDS = "zookeeper.scheduler.thresholds";
  
  // How many past calls to remember
  private int historyLength;
  private int[] thresholds;
  private int numQueues;

  public HistoryRequestScheduler() {
    this.callHistory = new ConcurrentLinkedQueue<Object>();
    this.callHistoryCounts = new ConcurrentHashMap<Object, AtomicInteger>();
  }

  public HistoryRequestScheduler(int numQueues) {
    this();

    this.numQueues = numQueues;
    if (numQueues <= 0) {
      throw new IllegalArgumentException("Number of queues must be positive.");
    }

    this.historyLength = Integer.getInteger(HISTORY_LENGTH, 10000);
	if (this.historyLength < 1) {
		throw new IllegalArgumentException("historylength must be at least 1");
	}
    String hold = System.getProperty(THRESHOLDS, "100,1000");
	  String[] holds = hold.split(",");
	  thresholds = new int[holds.length];
	  for(int i = 0; i < holds.length; i++){
	  	this.thresholds[i] = Integer.parseInt(holds[i]);
	  }
	  if(this.thresholds.length == 0) {
	  	this.thresholds = getDefaultThresholds(numQueues, this.historyLength);
	  }else if (this.thresholds.length != this.numQueues -1) {
	  	throw new IllegalArgumentException(
			THRESHOLDS + "must specify exactly " + (this.numQueues -1) + " weights: one for each priority level"
		);
	  }

    LOG.info("HistoryReuqestScheduler is being used.");
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

    for(int i=0; i < retval.length; i++) {
      retval[i] = (i + 1) * delta;
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
	InetAddress identity = req.cnxn.getRemoteSocketAddress().getAddress();
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
