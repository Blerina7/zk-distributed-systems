package queue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * DistributedQueue - Implementimi kryesor i queue-s se shperndarë.
 *
 * Si funksionon:
 * ─────────────
 * Zookeeper ruan te dhënat si nje "pemë" (tree) znodes-sh.
 * Ne krijojmë nje znode-prind:  /distributed-queue
 * Çdo mesazh behet nje znode-femijë: /distributed-queue/msg-0000000001
 *
 * Numrat jane sekuencial (SEQUENTIAL) - Zookeeper i shton automatikisht.
 * Kjo garanton RENDIN: msg-0001 konsumohет para msg-0002.
 *
 * Struktura ne Zookeeper:
 *   /distributed-queue/
 *       msg-0000000001   <- mesazhi i parë (konsumohет i pari - FIFO)
 *       msg-0000000002
 *       msg-0000000003
 *
 * FIFO = First In, First Out (i pari qe hyn, i pari qe del)
 */
public class DistributedQueue implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DistributedQueue.class);

    // Rruga ne Zookeeper ku ruhen mesazhet
    private static final String QUEUE_PATH = "/xhoi-distributed-queue";
    // Prefiksi i cdo mesazhi-znode
    private static final String MSG_PREFIX  = QUEUE_PATH + "/g3c-msg-";

    private final CuratorFramework client;

    /**
     * Konstruktori - krijon lidhjen me Zookeeper.
     *
     * @param zookeeperConnectionString  p.sh. "localhost:2181" ose "node1:2181,node2:2181,node3:2181"
     */
    public DistributedQueue(String zookeeperConnectionString) throws Exception {
        // ExponentialBackoffRetry: nëse lidhja dështon, provon persëri
        // me vonesa: 1s, 2s, 4s, 8s... deri ne 3 herë
        this.client = CuratorFrameworkFactory.newClient(
                zookeeperConnectionString,
                new ExponentialBackoffRetry(1000, 3)
        );
        this.client.start();

        // Presim deri sa lidhemi vertet
        this.client.blockUntilConnected();
        log.info("U lidh me Zookeeper: {}", zookeeperConnectionString);

        // Krijo znode-n prind nëse nuk ekziston
        ensureQueuePathExists();
    }

    /**
     * Konstruktori alternativ - merr CuratorFramework te gatshëm (perdoret ne teste).
     */
    public DistributedQueue(CuratorFramework client) throws Exception {
        this.client = client;
        ensureQueuePathExists();
    }

    /**
     * Siguron qe /distributed-queue ekziston ne Zookeeper.
     * Nëse nuk ekziston, e krijon.
     */
    private void ensureQueuePathExists() throws Exception {
        Stat stat = client.checkExists().forPath(QUEUE_PATH);
        if (stat == null) {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)   // mbetet edhe nese klienti shkëputet
                    .forPath(QUEUE_PATH, new byte[0]);
            log.info("Krijova queue path: {}", QUEUE_PATH);
        }
    }

    /**
     * ENQUEUE - Fut nje mesazh ne queue.
     *
     * Krijon nje znode SEQUENTIAL dhe PERSISTENT:
     *   - SEQUENTIAL: Zookeeper shton automatikisht nje numër unik ne fund
     *   - PERSISTENT: mbetet ne Zookeeper edhe nese producer-i shkëputet
     *
     * @param message  mesazhi qe duam te fusim
     * @return path-i i znode qe u krijua (p.sh. /distributed-queue/msg-0000000001)
     */
    public String enqueue(QueueMessage message) throws Exception {
        byte[] data = message.toBytes();

        String createdPath = client.create()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath(MSG_PREFIX, data);

        log.info("[ENQUEUE] Producer={} | MsgID={} | Path={}",
                message.getProducerId(), message.getId(), createdPath);
        return createdPath;
    }

    /**
     * DEQUEUE - Mer dhe heq mesazhin e parë nga queue (FIFO).
     *
     * Hapat:
     * 1. Liston te gjitha mesazhet-femijë
     * 2. Rendit ato me numër (p.sh. msg-0001 para msg-0002)
     * 3. Lexon mesazhin me numrin me te vogël (i pari)
     * 4. Fshin znode-n
     * 5. Kthen mesazhin
     *
     * Nëse queue eshte bosh, kthen null.
     *
     * @return mesazhi i parë, ose null nëse queue eshte bosh
     */
    public QueueMessage dequeue() throws Exception {
        while (true) {
            // Merr listen e te gjitha mesazheve
            List<String> children = client.getChildren().forPath(QUEUE_PATH);

            if (children.isEmpty()) {
                log.debug("[DEQUEUE] Queue eshte bosh");
                return null;
            }

            // Rendit sipas numrit sekuencial (FIFO - i pari qe hyri, i pari qe del)
            Collections.sort(children);

            // Merr mesazhin e parë
            String firstChild = children.get(0);
            String fullPath = QUEUE_PATH + "/" + firstChild;

            try {
                // Lexo te dhenat e znode
                byte[] data = client.getData().forPath(fullPath);

                // Fshij znode (konsumoi mesazhin)
                client.delete().forPath(fullPath);

                QueueMessage message = QueueMessage.fromBytes(data);
                log.info("[DEQUEUE] Konsumova: path={} | msg={}", fullPath, message);
                return message;

            } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
                // Nje consumer tjeter e konsumoi para nesh - provojme persëri
                log.warn("[DEQUEUE] Race condition - dikush tjeter mori mesazhin '{}', duke u provuar persëri...", firstChild);
                // Vazhdojmë loop-in dhe provojmë mesazhin tjeter
            }
        }
    }

    /**
     * Kthen numrin e mesazheve ne queue.
     */
    public int size() throws Exception {
        List<String> children = client.getChildren().forPath(QUEUE_PATH);
        return children.size();
    }

    /**
     * Kontrollon nëse queue eshte bosh.
     */
    public boolean isEmpty() throws Exception {
        return size() == 0;
    }

    /**
     * Mbyll lidhjen me Zookeeper.
     * Perdoret me try-with-resources.
     */
    @Override
    public void close() {
        if (client != null) {
            client.close();
            log.info("Lidhja me Zookeeper u mbyll.");
        }
    }
}
