package smith.melton.concur

import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import java.util.{Map=>JMap}

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentLinkedDeque}

/**
 * @author Melton Smith
 * @since 14.06.2025
 */
trait ConcurProcessor[K,V] {
  val productQueue: ArrayBlockingQueue[ConsumerRecords[K, V]]
  val offsetQueue: ConcurrentLinkedDeque[JMap[TopicPartition, OffsetAndMetadata]]


  def start(): Unit
  def processRecords(records: ConsumerRecords[K, V]) : Unit
  def getOffsets(): JMap[TopicPartition, OffsetAndMetadata]
  def close(): Unit
}
