package queue;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * QueueMessage - Nje mesazh ne distributed queue.
 *
 * Ky objekt serializohet ne bytes dhe ruhet si znode ne Zookeeper.
 * Cdo mesazh ka:
 *   - id:        identifikues unik
 *   - content:   permbajtja e mesazhit
 *   - timestamp: koha kur u krijua
 *   - producerId: kush e krijoi
 */
public class QueueMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String id;
    private final String content;
    private final long timestamp;
    private final String producerId;

    public QueueMessage(String id, String content, String producerId) {
        this.id = id;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
        this.producerId = producerId;
    }

    // --- Getters ---
    public String getId()         { return id; }
    public String getContent()    { return content; }
    public long   getTimestamp()  { return timestamp; }
    public String getProducerId() { return producerId; }

    /**
     * Kthen mesazhin ne byte array per ta ruajtur ne Zookeeper.
     * Perdorim serialization te thjeshte JSON-style si string.
     */
    public byte[] toBytes() {
        // Format: id|content|timestamp|producerId
        String serialized = id + "|" + content + "|" + timestamp + "|" + producerId;
        return serialized.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Krijon QueueMessage nga bytes te lexuara nga Zookeeper.
     */
    public static QueueMessage fromBytes(byte[] bytes) {
        String serialized = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = serialized.split("\\|", 4);
        if (parts.length < 4) {
            throw new IllegalArgumentException("Format i gabuar i mesazhit: " + serialized);
        }
        // Krijojme objektin duke vendosur timestamp manualisht
        QueueMessage msg = new QueueMessage(parts[0], parts[1], parts[3]);
        return msg;
    }

    @Override
    public String toString() {
        return String.format("QueueMessage{id='%s', producer='%s', content='%s', time=%d}",
                id, producerId, content, timestamp);
    }
}
