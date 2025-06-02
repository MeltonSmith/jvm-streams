package smith.melton.util

import org.apache.kafka.clients.producer.RecordMetadata

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

/**
 * @author Melton Smith
 * @since 01.06.2025
 */
object RecordMetadataUtil {

  private val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss:SSS");

  def prettyPrinter(recordMetadata: RecordMetadata): Unit = {
    if (recordMetadata != null) {
      val localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(recordMetadata.timestamp()), ZoneId.of("UTC"))
      System.out.printf("Topic: %s - Partition: %d - Offset: %d, UTC TimeStamp: %s\n", recordMetadata.topic, recordMetadata.partition, recordMetadata.offset, localDateTime.format(formatter))

    }
  }
}
