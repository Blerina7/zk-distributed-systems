// package ku ndodhet klasa
package com.zookeeper;

// sjell JUnit per testet
import org.junit.jupiter.api.Test;

// sjell funksionet e testimit (assertEquals, assertTrue, etj)
import static org.junit.jupiter.api.Assertions.*;

// sjell klasa per thread-e dhe concurrency
import java.util.concurrent.*;

// atomic integer qe eshte safe ne multi-threading
import java.util.concurrent.atomic.AtomicInteger;

// klasa kryesore e testeve per distributed lock
public class DistributedLockTest {

    // URL e ZooKeeper cluster
    // punon si ne Docker ashtu edhe ne localhost
    private static final String ZK_ADDRESS =
            System.getenv("ZK_CONNECTION") != null
                    ? System.getenv("ZK_CONNECTION")
                    : "localhost:2181";

    // TEST 1: kontrollon mutual exclusion
    @Test
    public void testMutualExclusion() throws Exception {

        // numri i thread-eve qe punojne njekohesisht
        int threadCount = 5;

        // tregon sa thread jane duke punuar ne te njejten kohe
        AtomicInteger current = new AtomicInteger(0);

        // mban maksimumin e concurrency
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        // numeron sa ekzekutime jane bere
        AtomicInteger counter = new AtomicInteger(0);

        // krijon thread pool me 5 thread-e
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // pret derisa te mbarojne te gjitha thread-et
        CountDownLatch done = new CountDownLatch(threadCount);

        // krijon thread per cdo pune
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {

                // objekti i distributed lock
                CuratorDistributedLock lock = null;

                try {
                    // krijon lock ne ZooKeeper
                    lock = new CuratorDistributedLock(ZK_ADDRESS, "/test-mutex");

                    // merr lock-un (bllokon te tjeret)
                    lock.acquire();

                    // rrit numrin e thread-eve aktive
                    int c = current.incrementAndGet();

                    // ruan maksimumin e concurrency
                    maxConcurrent.updateAndGet(max -> Math.max(max, c));

                    // simulon pune (100ms)
                    Thread.sleep(100);

                    // ul numrin e thread-eve aktive
                    current.decrementAndGet();

                    // rrit counter-in total
                    counter.incrementAndGet();

                    // liron lock-un
                    lock.release();

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                    // mbyll lock-un
                    if (lock != null) lock.close();

                    // shenon qe thread perfundoi
                    done.countDown();
                }
            });
        }

        // pret derisa te perfundojne te gjithe thread-et
        done.await(30, TimeUnit.SECONDS);

        // mbyll executor
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // printon max concurrency
        System.out.println("Max njekohesisht: " + maxConcurrent.get());

        // printon total ekzekutime
        System.out.println("Total ekzekutime: " + counter.get());

        // kontrollon qe vetem 1 thread ka punuar njekohesisht
        assertEquals(1, maxConcurrent.get(), "Mutual exclusion u shkel!");

        // kontrollon qe te gjithe thread-et kane ekzekutuar
        assertEquals(threadCount, counter.get(), "Jo te gjithe ekzekutuan!");

        System.out.println("TEST 1 KALOI");
    }

    // TEST 2: kontrollon nese lock lirohet pas crash
    @Test
    public void testLockReleaseOnCrash() throws Exception {

        // krijon lock te pare
        CuratorDistributedLock lock1 =
                new CuratorDistributedLock(ZK_ADDRESS, "/test-crash");

        // merr lock
        lock1.acquire();

        System.out.println("Lock 1 mori lock-un");

        // simulon crash duke mbyllur connection
        lock1.close();

        System.out.println("Lock 1 u mbyll (crash)");

        // krijon lock te dyte
        CuratorDistributedLock lock2 =
                new CuratorDistributedLock(ZK_ADDRESS, "/test-crash");

        // provon te marre lock brenda 5 sekondave
        boolean acquired = lock2.tryAcquire(5, TimeUnit.SECONDS);

        // kontrollon qe lock eshte marre
        assertTrue(acquired, "Lock 2 nuk mori lock-un pas crash-it!");

        // liron lock
        lock2.release();

        // mbyll lock
        lock2.close();

        System.out.println("TEST 2 KALOI");
    }

    // TEST 3: kontrollon race condition
    @Test
    public void testNoRaceCondition() throws Exception {

        // counter i perbashket
        AtomicInteger sharedCounter = new AtomicInteger(0);

        // numri i thread-eve
        int threadCount = 10;

        // thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // pret perfundimin e thread-eve
        CountDownLatch done = new CountDownLatch(threadCount);

        // krijon thread per cdo pune
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {

                CuratorDistributedLock lock = null;

                try {
                    // krijon lock
                    lock = new CuratorDistributedLock(ZK_ADDRESS, "/test-race");

                    // merr lock
                    lock.acquire();

                    // lexon vleren aktuale
                    int val = sharedCounter.get();

                    // simulon vonese
                    Thread.sleep(10);

                    // rrit vleren
                    sharedCounter.set(val + 1);

                    // liron lock
                    lock.release();

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {

                    // mbyll lock
                    if (lock != null) lock.close();

                    // shenon perfundimin
                    done.countDown();
                }
            });
        }

        // pret perfundimin
        done.await(60, TimeUnit.SECONDS);

        // mbyll executor
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);

        // printon rezultatin final
        System.out.println("Counter final: " + sharedCounter.get());

        // kontrollon qe nuk ka race condition
        assertEquals(threadCount, sharedCounter.get(),
                "Race condition! Vlera finale nuk eshte e sakte");

        System.out.println("TEST 3 KALOI");
    }
}