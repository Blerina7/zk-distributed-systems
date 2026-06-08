package queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // 🔥 Safe për Docker + Local
    private static final String ZK_CONNECTION =
            System.getenv().getOrDefault(
                    "ZK_CONNECTION",
                    "BlerinaNode:2181,ElisaNode:2181,XhesildaNode:2181,XhoanaNode:2181"
            );

    public static void main(String[] args) throws Exception {

        System.out.println("DK - 3. Distributed Queue");

        skenar1_Baze();
        Thread.sleep(2000);

        skenar2_Concurrent();
        Thread.sleep(2000);

        skenar3_StressTest();
    }

    // -------------------- SKENAR 1 --------------------
    static void skenar1_Baze() throws Exception {

        System.out.println("\n── SKENAR 1: Baze (1 Producer + 1 Consumer) ──\n");
        Consumer.resetCounter();

        try (DistributedQueue queue = new DistributedQueue(ZK_CONNECTION)) {

            int totalMessages = 5;

            Thread producerThread = new Thread(
                    new Producer("PRODUCER-1", queue, totalMessages, 100)
            );

            Thread consumerThread = new Thread(
                    new Consumer("CONSUMER-1", queue, totalMessages, 200)
            );

            producerThread.start();
            consumerThread.start();

            producerThread.join(30_000);
            consumerThread.join(30_000);

            System.out.printf(
                    "\n✔ Skenar 1: %d prodhuara / %d konsumuara\n",
                    totalMessages,
                    Consumer.getTotalConsumed()
            );
        }
    }

    // -------------------- SKENAR 2 --------------------
    static void skenar2_Concurrent() throws Exception {

        System.out.println("\n── SKENAR 2: Concurrent ──\n");
        Consumer.resetCounter();

        try (DistributedQueue queue = new DistributedQueue(ZK_CONNECTION)) {

            int msgsPerProducer = 10;
            int producers = 3;
            int consumers = 2;
            int totalMessages = msgsPerProducer * producers;

            ExecutorService executor =
                    Executors.newFixedThreadPool(producers + consumers);

            List<Consumer> consumerList = new ArrayList<>();

            for (int i = 1; i <= producers; i++) {
                executor.submit(
                        new Producer("PRODUCER-" + i, queue, msgsPerProducer, 50)
                );
            }

            for (int i = 1; i <= consumers; i++) {
                Consumer c = new Consumer("CONSUMER-" + i, queue, 0, 100);
                consumerList.add(c);
                executor.submit(c);
            }

            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);

            int totalConsumed = consumerList.stream()
                    .mapToInt(Consumer::getConsumedCount)
                    .sum();

            System.out.printf(
                    "\n✔ Skenar 2: %d prodhuara / %d konsumuara\n",
                    totalMessages,
                    totalConsumed
            );
        }
    }

    // -------------------- SKENAR 3 --------------------
    static void skenar3_StressTest() throws Exception {

        System.out.println("\n── SKENAR 3: Stress Test ──\n");
        Consumer.resetCounter();

        try (DistributedQueue queue = new DistributedQueue(ZK_CONNECTION)) {

            int msgsPerProducer = 20;
            int producers = 5;
            int consumers = 3;
            int totalMessages = msgsPerProducer * producers;

            ExecutorService executor =
                    Executors.newFixedThreadPool(producers + consumers);

            long start = System.currentTimeMillis();

            for (int i = 1; i <= producers; i++) {
                executor.submit(
                        new Producer("PRODUCER-" + i, queue, msgsPerProducer, 0)
                );
            }

            List<Consumer> consumerList = new ArrayList<>();

            for (int i = 1; i <= consumers; i++) {
                Consumer c = new Consumer("CONSUMER-" + i, queue, 0, 50);
                consumerList.add(c);
                executor.submit(c);
            }

            executor.shutdown();
            executor.awaitTermination(120, TimeUnit.SECONDS);

            long duration = System.currentTimeMillis() - start;

            int totalConsumed = consumerList.stream()
                    .mapToInt(Consumer::getConsumedCount)
                    .sum();

            System.out.printf("\n✔ Stress Test përfundoi\n");
            System.out.printf("   Prodhuara: %d / Konsumuara: %d\n",
                    totalMessages, totalConsumed);
            System.out.printf("   Koha: %d ms\n", duration);
            System.out.printf("   Throughput: %.2f msg/sec\n",
                    totalConsumed / (duration / 1000.0));
        }
    }
}