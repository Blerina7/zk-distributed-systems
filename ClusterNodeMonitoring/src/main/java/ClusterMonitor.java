import org.apache.zookeeper.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterMonitor implements Watcher {

    private ZooKeeper zk;

    private static final String CONNECT_STRING =
            System.getenv("ZK_CONNECTION") != null
                    ? System.getenv("ZK_CONNECTION")
                    : "BlerinaNode:2181,ElisaNode:2181,XhesildaNode:2181,XhoanaNode:2181";

    private static final int SESSION_TIMEOUT = 60000;
    private final CountDownLatch connectedLatch = new CountDownLatch(1);

    private static final String[] NODES = {
            "BlerinaNode", "ElisaNode", "XhesildaNode", "XhoanaNode"
    };

    public void connect() throws Exception {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, this);
        if (!connectedLatch.await(60, TimeUnit.SECONDS)) {
            throw new RuntimeException("Nuk u lidh me ZooKeeper!");
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedLatch.countDown();
        }
    }

    public void showLiveNodes() {
        System.out.println("┌─────────────────────────────┐");
        System.out.println("│       CLUSTER STATUS        │");
        System.out.println("├─────────────────────────────┤");
        for (String node : NODES) {
            boolean alive;
            try (java.net.Socket s = new java.net.Socket()) {
                s.connect(new java.net.InetSocketAddress(node, 2181), 2000);
                alive = true;
            } catch (Exception e) {
                alive = false;
            }
            String status = alive ? "🟢 ONLINE " : "🔴 OFFLINE";
            System.out.printf("│  %-14s  %s │%n", node, status);
        }
        System.out.println("└─────────────────────────────┘");
    }

    public static void main(String[] args) throws Exception {

        ClusterMonitor monitor = new ClusterMonitor();
        monitor.connect();
        System.out.println("✔ I lidhur me ZooKeeper!");

        monitor.showLiveNodes();

        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(monitor::showLiveNodes, 10, 10, TimeUnit.SECONDS);

        Thread.sleep(Long.MAX_VALUE);
    }
}