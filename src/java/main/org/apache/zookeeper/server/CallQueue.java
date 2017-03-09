package org.apache.zookeeper.server;

public interface CallQueue {
    void put(Request req) throws InterruptedException;
    Request takr() throws InterruptedException;
    int size();
    coid clear();
}
