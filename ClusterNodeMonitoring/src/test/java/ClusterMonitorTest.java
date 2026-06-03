// sjell te gjitha klasat e ZooKeeper (connect, create, watch, etj)
import org.apache.zookeeper.*;

// sjell JUnit per testet (BeforeEach, Test, etj)
import org.junit.jupiter.api.*;

// perdoret per lista (p.sh. liste e node-ve)
import java.util.List;

// perdoret per te pritur derisa ZooKeeper te lidhet
import java.util.concurrent.CountDownLatch;

// perdoret per kohe (sekonda, milisekonda)
import java.util.concurrent.TimeUnit;

// sjell funksionet e testimit si assertTrue, assertEquals
import static org.junit.jupiter.api.Assertions.*;

// klasa kryesore ku behen testet
public class ClusterMonitorTest {

    // perdor localhost:2181 jo Docker
    private static final String CONNECT_STRING = "localhost:2181";

    // koha maksimale per lidhje me ZooKeeper (5 sekonda)
    private static final int SESSION_TIMEOUT = 5000;

    // objekti kryesor qe lidhet me ZooKeeper
    private ZooKeeper zk;

    // ky kod ekzekutohet PARA cdo testi
    @BeforeEach
    void setUp() throws Exception {

        // krijon nje "bllokues" qe pret lidhjen
        CountDownLatch latch = new CountDownLatch(1);

        // krijon lidhjen me ZooKeeper
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, event -> {

            // nese lidhja u realizua me sukses
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {

                // hap bllokuesin (jemi connected)
                latch.countDown();
            }
        });

        // pret maksimum 10 sekonda per lidhje
        assertTrue(
                latch.await(10, TimeUnit.SECONDS),
                "Nuk u lidh me ZooKeeper ne localhost"
        );

        // kontrollon nese root node ekziston
        if (zk.exists("/live_nodes", false) == null) {

            // nese nuk ekziston e krijon
            zk.create(
                    "/live_nodes",
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT
            );
        }
    }

    // ky kod ekzekutohet PAS cdo testi
    @AfterEach
    void tearDown() throws Exception {

        try {
            // merr te gjitha node brenda /live_nodes
            List<String> children = zk.getChildren("/live_nodes", false);

            // kalon nje nga nje
            for (String c : children) {

                // i fshin te gjithe node
                zk.delete("/live_nodes/" + c, -1);
            }

        } catch (Exception ignored) {
            // nese ka error e injoron
        }

        // mbyll lidhjen me ZooKeeper
        if (zk != null) {
            zk.close();
        }
    }

    // teston nese jemi te lidhur
    @Test
    void testConnection() {

        // kontrollon statusin e lidhjes
        assertEquals(ZooKeeper.States.CONNECTED, zk.getState());
    }

    // teston krijimin e nje node
    @Test
    void testNodeRegistration() throws Exception {

        // krijon nje node te perkohshme
        zk.create(
                "/live_nodes/BlerinaNode",
                "ok".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
        );

        // merr listen e node
        List<String> children = zk.getChildren("/live_nodes", false);

        // kontrollon nese ekziston node
        assertTrue(children.contains("BlerinaNode"));
    }

    // teston watcher kur shtohet node
    @Test
    void testWatcherTriggeredOnNodeAdd() throws Exception {

        // pret event
        CountDownLatch latch = new CountDownLatch(1);

        // vendos watcher
        zk.getChildren("/live_nodes", event -> {

            // nese ndryshon lista e node
            if (event.getType() ==
                    Watcher.Event.EventType.NodeChildrenChanged) {

                // aktivizo testin
                latch.countDown();
            }
        });

        // krijon node te ri
        zk.create(
                "/live_nodes/ElisaNode",
                "ok".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL
        );

        // pret 5 sekonda per event
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    // teston krijimin e disa node
    @Test
    void testMultipleNodes() throws Exception {

        // lista e emrave
        String[] nodes = {"A", "B", "C", "D"};

        // per cdo emer
        for (String n : nodes) {

            // krijon node
            zk.create(
                    "/live_nodes/" + n,
                    "ok".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
            );
        }

        // merr te gjitha node
        List<String> children = zk.getChildren("/live_nodes", false);

        // kontrollon sa jane
        assertEquals(4, children.size());
    }
}