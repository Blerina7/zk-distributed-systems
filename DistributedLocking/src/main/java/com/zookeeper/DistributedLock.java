package com.zookeeper;

public interface DistributedLock {
    void acquire() throws Exception;
    void release() throws Exception;
}