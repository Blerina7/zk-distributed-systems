package queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer - Lexon dhe perpunon mesazhe nga DistributedQueue.
 *
 * Consumer ekzekutohet ne loop:
 *   1. Provon te marre nje mesazh (dequeue)
 *   2. Nese gjej, e perpunon
 *   3. Nese nuk gjej, pret pak dhe provon persëri
 *   4. Ndalet kur totalMsgsExpected mesazhe jane konsumuar
 *      (ose kur thread-i nderpritet)
 */
public class Consumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private final String consumerId;
    private final DistributedQueue queue;
    private final int totalMsgsExpected;   // sa mesazhe pret te konsumoje
    private final long pollIntervalMs;     // sa shpesh kontrollon queue-n (ms)

    // Mesazhet e konsumuar nga ky consumer
    private final List<QueueMessage> consumedMessages = Collections.synchronizedList(new ArrayList<>());

    // Numri total i mesazheve te konsumuar (ndahet mes Consumers)
    private static final AtomicInteger totalConsumed = new AtomicInteger(0);

    /**
     * @param consumerId        ID e konsumuesit (p.sh. "Consumer-1")
     * @param queue             referenca e queue-s se shperndarë
     * @param totalMsgsExpected sa mesazhe priten gjithsej (te gjithë consumers)
     * @param pollIntervalMs    vonesa mes kontrollimeve kur queue eshte bosh (ms)
     */
    public Consumer(String consumerId, DistributedQueue queue,
                    int totalMsgsExpected, long pollIntervalMs) {
        this.consumerId        = consumerId;
        this.queue             = queue;
        this.totalMsgsExpected = totalMsgsExpected;
        this.pollIntervalMs    = pollIntervalMs;
    }

    /**
     * Ekzekutohet kur thread-i starton.
     * Konsumoje mesazhe deri sa:
     *   a) totalConsumed arrin totalMsgsExpected, ose
     *   b) thread-i nderpritet
     */
    @Override
    public void run() {
        log.info("[{}] Duke filluar. Pret {} mesazhe gjithsej.", consumerId, totalMsgsExpected);

        while (!Thread.currentThread().isInterrupted()) {

            // Kontrollojme nese kemi konsumuar te gjithë mesazhet e pritura
            if (totalConsumed.get() >= totalMsgsExpected) {
                log.info("[{}] Te gjithë mesazhet ({}) u konsumuan. Ne ndalim...",
                        consumerId, totalMsgsExpected);
                break;
            }

            try {
                // Mer mesazhin e parë nga queue
                QueueMessage message = queue.dequeue();

                if (message != null) {
                    // Mesazh i gjetur - e perpunojme
                    consumedMessages.add(message);
                    int count = totalConsumed.incrementAndGet();

                    log.info("[{}] Konsumova mesazhin {}/{}: {}",
                            consumerId, count, totalMsgsExpected, message.getId());

                    // Simulojme perpunimin (p.sh. shkruajme ne DB)
                    processMessage(message);

                } else {
                    // Queue eshte bosh - presim pak para se te provojme persëri
                    log.debug("[{}] Queue eshte bosh. Duke pritur {}ms...",
                            consumerId, pollIntervalMs);
                    Thread.sleep(pollIntervalMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[{}] U nderpre.", consumerId);
                break;
            } catch (Exception e) {
                log.error("[{}] Gabim gjate konsumimit: {}", consumerId, e.getMessage());
            }
        }

        log.info("[{}] Perfundoi. Konsumova {} mesazhe.",
                consumerId, consumedMessages.size());
    }

    /**
     * Perpunon mesazhin - ne implementimin real ketu do te benim:
     * - Shkruajmë ne databaze
     * - Dergojmë email
     * - Procesojmë pagesen, etj.
     *
     * Ketu simulojme me nje vonese te vogël.
     */
    private void processMessage(QueueMessage message) throws InterruptedException {
        // Simulon 50ms pune per mesazh
        Thread.sleep(50);
        log.debug("[{}] Perpunova: {}", consumerId, message.getContent());
    }

    // --- Getters per statistika ---

    public String getConsumerId() { return consumerId; }

    public List<QueueMessage> getConsumedMessages() {
        return Collections.unmodifiableList(consumedMessages);
    }

    public int getConsumedCount() { return consumedMessages.size(); }

    public static int getTotalConsumed() { return totalConsumed.get(); }  // ← ADD THIS

    /** Resetimi i counter-it global (perdoret ne teste) */
    public static void resetCounter() { totalConsumed.set(0); }
}
