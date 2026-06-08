package queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Producer - Gjeneron dhe fut mesazhe ne DistributedQueue.
 *
 * Çdo Producer ka nje ID unik dhe mund te ekzekutohet
 * ne nje thread te veçante (concurrent producer).
 *
 * Perdorim AtomicInteger per te numruar mesazhet - eshte
 * thread-safe (i sigurt kur shume threads e perdorin njekohesisht).
 */
public class Producer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final String producerId;
    private final DistributedQueue queue;
    private final int messagesToProduce;   // sa mesazhe do te prodhoje
    private final long delayMs;            // vonesa mes mesazheve (ms)

    // Numeron mesazhet globalisht (ndahet mes te gjithë Producers)
    private static final AtomicInteger globalCounter = new AtomicInteger(0);

    /**
     * @param producerId        ID e prodhuesit (p.sh. "Producer-1")
     * @param queue             referenca e queue-s se shperndarë
     * @param messagesToProduce sa mesazhe do te dergoje
     * @param delayMs           vonesa mes mesazheve ne milisekonda
     */
    public Producer(String producerId, DistributedQueue queue,
                    int messagesToProduce, long delayMs) {
        this.producerId       = producerId;
        this.queue            = queue;
        this.messagesToProduce = messagesToProduce;
        this.delayMs          = delayMs;
    }

    /**
     * Ekzekutohet kur thread-i starton.
     * Krijon dhe fut 'messagesToProduce' mesazhe ne queue.
     */
    @Override
    public void run() {
        log.info("[{}] Duke filluar. Do te prodhoje {} mesazhe.", producerId, messagesToProduce);

        for (int i = 0; i < messagesToProduce; i++) {
            try {
                // Krijo ID unik per mesazhin
                String msgId = producerId + "-msg-" + globalCounter.incrementAndGet();

                // Permbajtja e mesazhit
                String content = "Mesazh #" + globalCounter.get() +
                                 " nga " + producerId +
                                 " [" + System.currentTimeMillis() + "]";

                QueueMessage message = new QueueMessage(msgId, content, producerId);

                // Fut ne queue
                queue.enqueue(message);

                log.info("[{}] Futa mesazhin {}/{}: {}",
                         producerId, i + 1, messagesToProduce, msgId);

                // Vones mes mesazheve (simulon punen reale)
                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[{}] U nderpre.", producerId);
                break;
            } catch (Exception e) {
                log.error("[{}] Gabim gjate futjes se mesazhit: {}", producerId, e.getMessage());
            }
        }

        log.info("[{}] Perfundoi prodhimin.", producerId);
    }

    public String getProducerId() { return producerId; }
}
