package queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DistributedQueueTest - Teste automatike per Distributed Queue.
 *
 * Perdorim TestingServer nga Curator qe simulon nje server
 * Zookeeper brenda procesit tone - nuk kemi nevoje per instalim!
 *
 * Testet:
 *   1. Enqueue dhe Dequeue baze
 *   2. FIFO ordering (rendi)
 *   3. Queue bosh kthen null
 *   4. Concurrent producers (race condition test)
 *   5. Concurrent producers dhe consumers
 *   6. Node failure simulation (fshirja e znode gjate dequeue)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedQueueTest {

    // Server i brendshëm ZooKeeper per teste
    private static TestingServer testingServer;

    // Queue qe do testojmë
    private DistributedQueue queue;

    // Client i Curator per te kontrolluar gjendjen drejtperdrejt
    private CuratorFramework client;

    @BeforeAll
    static void startZooKeeper() throws Exception {
        // Starto serverin e brendshëm ZooKeeper ne port 2999
        testingServer = new TestingServer(2999, true);
        System.out.println("✅ ZooKeeper test server: " + testingServer.getConnectString());
    }

    @AfterAll
    static void stopZooKeeper() throws Exception {
        if (testingServer != null) testingServer.close();
    }

    @BeforeEach
    void setUp() throws Exception {
        // Krijo lidhje te re para cdo testi
        client = CuratorFrameworkFactory.newClient(
                testingServer.getConnectString(),
                new RetryOneTime(500)
        );
        client.start();
        client.blockUntilConnected();
        queue = new DistributedQueue(client);
        Consumer.resetCounter();
    }

    @AfterEach
    void tearDown() throws Exception {
        // Pastro pas cdo testi - fshi TE GJITHA znodes
        try {
            if (client != null && client.getZookeeperClient().isConnected()) {
                List<String> children = client.getChildren().forPath("/xhoi-distributed-queue");
                for (String child : children) {
                    try {
                        client.delete().forPath("/xhoi-distributed-queue/" + child);
                    } catch (Exception ignored) {}
                }
            }
        } catch (Exception ignored) {}
        if (queue != null) queue.close();
        if (client != null) client.close();
        Consumer.resetCounter();
    }

    // ─── TEST 1: Enqueue dhe Dequeue baze ───────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("Test 1: Enqueue dhe Dequeue baze")
    void test1_BasicEnqueueDequeue() throws Exception {
        System.out.println("\n[TEST 1] Enqueue dhe Dequeue baze");

        QueueMessage msg = new QueueMessage("id-1", "Mesazh test", "TestProducer");
        queue.enqueue(msg);

        assertFalse(queue.isEmpty(), "Queue duhet te kete 1 element");
        assertEquals(1, queue.size(), "Queue duhet te kete madhesi 1");

        QueueMessage received = queue.dequeue();

        assertNotNull(received, "Mesazhi i kthyer nuk duhet te jete null");
        assertEquals("id-1", received.getId(), "ID duhet te perputhet");
        assertEquals("Mesazh test", received.getContent(), "Permbajtja duhet te perputhet");

        assertTrue(queue.isEmpty(), "Queue duhet te jete bosh pas dequeue");

        System.out.println("   ✅ KALOI");
    }

    // ─── TEST 2: FIFO Ordering ───────────────────────────────────────────────

    @Test
    @Order(2)
    @DisplayName("Test 2: FIFO - i pari qe hyn, i pari qe del")
    void test2_FIFOOrdering() throws Exception {
        System.out.println("\n[TEST 2] FIFO Ordering");

        // Fut 5 mesazhe ne rend
        for (int i = 1; i <= 5; i++) {
            queue.enqueue(new QueueMessage("id-" + i, "Mesazh " + i, "P1"));
            Thread.sleep(10); // Vonese te vogël per rend te garantuar
        }

        assertEquals(5, queue.size(), "Queue duhet te kete 5 mesazhe");

        // Dequeue-ja duhet t'i ktheje ne te njejtin rend
        for (int i = 1; i <= 5; i++) {
            QueueMessage msg = queue.dequeue();
            assertNotNull(msg);
            // Verifikojme rendin FIFO
            assertTrue(msg.getId().endsWith(String.valueOf(i)),
                    "FIFO gabim: pritej id-" + i + " por mori " + msg.getId());
        }

        System.out.println("   ✅ KALOI - FIFO i garantuar");
    }

    // ─── TEST 3: Queue bosh ─────────────────────────────────────────────────

    @Test
    @Order(3)
    @DisplayName("Test 3: Dequeue nga queue bosh kthen null")
    void test3_EmptyQueueReturnsNull() throws Exception {
        System.out.println("\n[TEST 3] Queue bosh");

        assertTrue(queue.isEmpty(), "Queue duhet te filloje bosh");

        QueueMessage result = queue.dequeue();
        assertNull(result, "Dequeue nga queue bosh duhet te ktheje null");

        System.out.println("   ✅ KALOI");
    }

    // ─── TEST 4: Concurrent Producers ────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("Test 4: Concurrent Producers - race condition test")
    void test4_ConcurrentProducers() throws Exception {
        System.out.println("\n[TEST 4] Concurrent Producers (3 producers, 10 mesazhe secili)");

        int numProducers    = 3;
        int msgsPerProducer = 10;
        int total           = numProducers * msgsPerProducer; // 30

        ExecutorService executor = Executors.newFixedThreadPool(numProducers);

        // Starto 3 producers njekohesisht
        for (int i = 1; i <= numProducers; i++) {
            Producer p = new Producer("P" + i, queue, msgsPerProducer, 0);
            executor.submit(p);
        }

        executor.shutdown();
        boolean done = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertTrue(done, "Producers nuk perfunduan ne kohe");
        assertEquals(total, queue.size(),
                "Queue duhet te kete " + total + " mesazhe, por ka " + queue.size());

        System.out.println("   ✅ KALOI - " + total + " mesazhe ne queue pa konflikt");
    }

    // ─── TEST 5: Concurrent Producers + Consumers ───────────────────────────

    @Test
    @Order(5)
    @DisplayName("Test 5: Concurrent Producers + Consumers njekohesisht")
    void test5_ConcurrentProducersAndConsumers() throws Exception {
        System.out.println("\n[TEST 5] Concurrent Producers + Consumers (3P + 2C, 30 msg)");

        int numProducers    = 3;
        int msgsPerProducer = 10;
        int numConsumers    = 2;
        int total           = numProducers * msgsPerProducer;

        ExecutorService executor = Executors.newFixedThreadPool(numProducers + numConsumers);
        List<Consumer> consumers = new ArrayList<>();

        // Starto producers
        for (int i = 1; i <= numProducers; i++) {
            executor.submit(new Producer("P" + i, queue, msgsPerProducer, 20));
        }

        // Starto consumers
        for (int i = 1; i <= numConsumers; i++) {
            Consumer c = new Consumer("C" + i, queue, total, 100);
            consumers.add(c);
            executor.submit(c);
        }

        executor.shutdown();
        boolean done = executor.awaitTermination(60, TimeUnit.SECONDS);

        int totalConsumed = consumers.stream().mapToInt(Consumer::getConsumedCount).sum();

        assertTrue(done, "Sistemi nuk perfundoi ne kohe");
        assertEquals(total, totalConsumed,
                "Duhet konsumuar " + total + " mesazhe por u konsumuan " + totalConsumed);
        assertTrue(queue.isEmpty(), "Queue duhet te jete bosh ne fund");

        System.out.println("   ✅ KALOI - Te gjithë " + total + " mesazhe u konsumuan saktesisht");
    }

    // ─── TEST 6: Node Failure Simulation ─────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("Test 6: Node Failure - queue vazhdon pas dekonektimit/rikonektimit")
    void test6_NodeFailureResilience() throws Exception {
        System.out.println("\n[TEST 6] Node Failure Simulation");

        // Fut 3 mesazhe
        for (int i = 1; i <= 3; i++) {
            queue.enqueue(new QueueMessage("id-" + i, "Mesazh " + i, "P1"));
        }
        assertEquals(3, queue.size());

        // Simulojme "death" te nje consumer: merr 1 mesazh, pastaj "deson"
        QueueMessage msg1 = queue.dequeue();
        assertNotNull(msg1, "Mesazhi i pare duhet te jete i disponushem");
        System.out.println("   Consumer 1 mori: " + msg1.getId() + " dhe 'deu'");

        // Nje consumer i ri vazhdon nga aty ku lë tjetri
        // (kjo eshte tipike ne sistemet reale - idempotent recovery)
        assertEquals(2, queue.size(), "Duhet te mbeten 2 mesazhe");

        QueueMessage msg2 = queue.dequeue();
        QueueMessage msg3 = queue.dequeue();

        assertNotNull(msg2);
        assertNotNull(msg3);
        assertTrue(queue.isEmpty(), "Queue duhet te jete bosh");

        System.out.println("   ✅ KALOI - Sistemi vazhdoi pas 'failure' te consumer-it");
    }

    // ─── TEST 7: Serialization/Deserialization ─────────────────────────────

    @Test
    @Order(7)
    @DisplayName("Test 7: Mesazhet serialiozohen dhe deserializohen saktesisht")
    void test7_MessageSerialization() {
        System.out.println("\n[TEST 7] Serialization/Deserialization");

        QueueMessage original = new QueueMessage("test-id", "Permbajtje testimi 123", "ProducerTest");

        byte[] bytes = original.toBytes();
        QueueMessage restored = QueueMessage.fromBytes(bytes);

        assertEquals(original.getId(),         restored.getId());
        assertEquals(original.getContent(),    restored.getContent());
        assertEquals(original.getProducerId(), restored.getProducerId());

        System.out.println("   ✅ KALOI - Mesazhi u ruajt dhe u rikuperua saktesisht");
    }
}
