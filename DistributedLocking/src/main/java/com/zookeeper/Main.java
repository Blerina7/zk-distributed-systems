package com.zookeeper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;

public class Main {

    private static final String ZK_CONNECTION =
            System.getenv().getOrDefault("ZK_CONNECTION", "Blerina1:2181");

    public static void main(String[] args) throws Exception {

        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch done = new CountDownLatch(threadCount);

        System.out.println("DK - 1. Distributed Lock");

        for (int i = 0; i < threadCount; i++) {
            final int id = i;

            executor.submit(() -> {
                CuratorDistributedLock lock = null;
                try {
                    lock = new CuratorDistributedLock(ZK_CONNECTION, "/bleri-lock");
                    lock.acquire();

                    System.out.println("Thread-" + id + " po punon...");
                    Thread.sleep(500);

                    lock.release();

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (lock != null) lock.close();
                    done.countDown();
                }
            });
        }

        done.await();
        executor.shutdown();

        System.out.println("U krye!");
    }
}