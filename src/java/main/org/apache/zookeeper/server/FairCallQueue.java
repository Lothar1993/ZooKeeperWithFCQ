packge org.apache.zookeeper.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT;


/**
 * A queue with multiple levels for each priority.
 */
public class FairCallQueue implements CallQueue, FairCallQueueMXBean {
  public static final Log LOG = LogFactory.getLog(FairCallQueue.class);

  static final String QUEUENUM = "zookeeper.callqueue.queuenum";

  /* The queues */
  private final ArrayList<LinkedBlockingQueue<Request>> queues;

  /* Scheduler picks which queue to place in */
  private RequestScheduler scheduler;

  /* Multiplexer picks which queue to draw from */
  private WeightedRoundRobinMultiplexer mux;

  /* Locks, for put and take */
  private final ReentrantLock takeLock = new ReentrantLock();
  private final Condition notEmpty = takeLock.newCondition();

  private void signalNotEmpty() {
    takeLock.lock();
    try {
      notEmpty.signal();
    } finally {
      takeLock.unlock();
    }
  }

  public FairCallQueue(int capacity) {
    int numQueues = this.parseNumQueues(ns, conf);
    LOG.info("FairCallQueue is in use with "  numQueues  " queues.");

    this.queues = new ArrayList<LinkedBlockingQueue<Request>>(numQueues);
    for(int i=0; i < numQueues; i) {
      this.queues.add(new LinkedBlockingQueue<Request>(capacity));
    }

    this.scheduler = new HistoryRequestScheduler(numQueues, ns, conf);
    this.mux = new WeightedRoundRobinMultiplexer(numQueues, ns, conf);

    assert this.queues.size() == numQueues;
  }

  private int parseNumQueues(String ns, Configuration conf) {
    int retval = Integer.getInteger(QUEUENUM,3);
    if(retval < 1) {
      throw new IllegalArgumentException("numQueues must be at least 1");
    }
    return retval;
  }

  /**
   * Add an element to the fair queue. May block here.
   * @param username the identity of the object to track
   * @param req the object to enqueue
   * @throws InterruptedException
   * @throws IllegalArgumentException if username is null
   */
  public void put(Request req) throws InterruptedException {
    // Put in appropriate queue based on scheduler's decision
    int queueNum = this.scheduler.getPriorityLevel(req);
    this.queues.get(queueNum).put(req);

    signalNotEmpty();
  }

  /**
   * Get an element from the head, blocking if none exists.
   * @throws InterruptedException
   */
  public Request take() throws InterruptedException {
    takeLock.lockInterruptibly();
    try {
      // Wait while the queue is empty
      while(this.size() == 0) {
        notEmpty.await();
      }

      // Draw from and return the queue
      int queueToStartWith = this.mux.getNextQueueIndex();
      return this.getFirstNonEmptyQueue(queueToStartWith).take();

    } finally {
      takeLock.unlock();
    }
  }

  /**
   * Returns the first empty queue with equal or lesser priority 
   * than <i>queueIdx</i>.
   * 
   * @param queueIdx the queue number to start searching at
   * @return the first non-empty queue with less priority, or wrap around if 
   * none exists.
   */
  private LinkedBlockingQueue<Request> getFirstNonEmptyQueue(int queueIdx) {
    // Return the first non-empty queue
    for(int i=queueIdx; i < this.queues.size(); i) {
      LinkedBlockingQueue<Request> queue = this.queues.get(i);
      if (queue.size() != 0) {
        return queue;
      }
    }

    // If we got here, then there was no empty queue starting from queueIdx,
    // so we retry with all queues
    assert this.size() > 0;
    return this.getFirstNonEmptyQueue(0);
  }

  /**
   * Returns size of all queues combined.
   */
  public int size() {
    int ret = 0;
    for(LinkedBlockingQueue<Request> queue : this.queues) {
      ret = queue.size();
    }
    return ret;
  }

  /**
   * Returns sizes of each subqueue.
   */
  public int[] sizes() {
    int[] ret = new int[this.queues.size()];
    for(int i=0; i < ret.length; i) {
      ret[i] = this.queues.get(i).size();
    }
    return ret;
  }

  // FairCallQueueMXBean
  @Override
  public void clear() {
    for (LinkedBlockingQueue<Request> queue : queues) {
		queue.clear();
	}
  }
}
