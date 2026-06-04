package com.zookeeper;

import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class CuratorDistributedLock implements DistributedLock {

    private final CuratorFramework client;
    private final InterProcessMutex mutex;

    public CuratorDistributedLock(String zkAddress, String lockPath) {

        System.out.println("Connecting to ZK: " + zkAddress);

        client = CuratorFrameworkFactory.newClient(
                zkAddress,
                new ExponentialBackoffRetry(1000, 3)
        );

        client.start();

        try {
            client.blockUntilConnected(); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        mutex = new InterProcessMutex(client, lockPath);
    }

    @Override
    public void acquire() throws Exception {
        System.out.println(Thread.currentThread().getName() + " waiting for lock...");
        mutex.acquire();
        System.out.println(Thread.currentThread().getName() + " got lock!");
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws Exception {
        return mutex.acquire(timeout, unit);
    }

    @Override
    public void release() throws Exception {
        if (mutex.isAcquiredInThisProcess()) {
            mutex.release();
            System.out.println(Thread.currentThread().getName() + " left lock-un.");
        }
    }

    public void close() {
        client.close();
    }
}