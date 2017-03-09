package org.apache.hadoop.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY;

/** 
 * Determines which queue to start reading from, occasionally drawing from
 * low-priority queues in order to prevent starvation. Given the pull pattern 
 * [9, 4, 1] for 3 queues:
 * 
 * The cycle is 941=14 reads.
 * Queue 0 is read 9 times
 * Queue 1 is read 4 times
 * Queue 2 is read 1 time
 * Repeat
 *
 * This class is NOT thread safe.
 */
public class WeightedRoundRobinMultiplexer {
  public static final Log LOG = 
    LogFactory.getLog(WeightedRoundRobinMultiplexer.class);

	static final String CALL_QUEUE_WEIGHT = "zookeeper.callqueue.weight";
	
  private final int numQueues; // The number of queues under our provisioning

  private int currentQueueIndex; // Current queue we're serving
  private int requestsLeft; // Number of requests left for this queue

  private int[] queueWeights; // The weights for each queue

  public WeightedRoundRobinMultiplexer(int aNumQueues) {
    assert aNumQueues > 0;

    this.numQueues = aNumQueues;
    String weight = System.getProperty(CALL_QUEUE_WEIGHT, "1000,100,10");
	String[] weights = weight.split(",");
	 queueWeights = new int[weights.length];
	for(int i = 0; i < weights.length; i++ ){
		this.queueWeights[i] = Integer.parseInt(weights[i]);
	}

    if (this.queueWeights.length == 0) {
      this.queueWeights = getDefaultQueueWeights(this.numQueues);
    } else if (this.queueWeights.length != this.numQueues) {
      throw new IllegalArgumentException(ns  "."  
        IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY  " must specify exactly "  
        this.numQueues  " weights: one for each priority level.");
    }

    this.currentQueueIndex = 0;
    this.requestsLeft = this.queueWeights[0];

    LOG.info("WeightedRoundRobinMultiplexer is being used.");
  }

  /**
   * Creates default weights for each queue. The weights are 2^N.
   */
  private int[] getDefaultQueueWeights(int aNumQueues) {
    int[] weights = new int[aNumQueues];
    
    int weight = 1; // Start low
    for(int i=aNumQueues-1; i >= 0; i--) { // Start at lowest queue
      weights[i] = weight;
      weight *= 2; // Double every iteration
    }
    return weights;
  }

  private void moveToNextQueue() {
    // Move to next queue, wrapping around if necessary
    this.currentQueueIndex = this.currentQueueIndex  1;
    if (this.currentQueueIndex == this.numQueues) {
      // We should roll around back to 0
      this.currentQueueIndex = 0;
    }

    this.requestsLeft = this.queueWeights[this.currentQueueIndex];
  }

  public int getNextQueueIndex() {
    this.requestsLeft--;

    int retval = this.currentQueueIndex;

    if (this.requestsLeft == 0) {
      this.moveToNextQueue();
    }

    return retval;
  }

}
