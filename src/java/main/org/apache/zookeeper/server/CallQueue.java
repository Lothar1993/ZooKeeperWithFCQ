package org.apache.zookeeper.server;

public interface CallQueue {
    void put(Request req) throws InterruptedException;
    Request take() throws InterruptedException;
    int size();
    void clear();
}
