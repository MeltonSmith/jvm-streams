package smith.melton.util

import org.apache.kafka.clients.producer.RecordMetadata

/**
 * @author Melton Smith
 * @since 01.06.2025
 */
object RecordMetadataUtil {
  def prettyPrinter(recordMetadata: RecordMetadata): Unit = {
    if (recordMetadata != null) System.out.printf("Topic: %s - Partition: %d - Offset: %d\n", recordMetadata.topic, recordMetadata.partition, recordMetadata.offset)
  }
}
