package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.{ProducerFencedException, WakeupException}
import org.apache.kafka.common.utils.{Exit, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.slf4j.LoggerFactory
import smith.melton.faker.user.User
import smith.melton.util.RecordMetadataUtil

import java.time.Duration
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Collections, Map => JMap}

/**
 * @author Melton Smith
 * @since 01.06.2025
 */
object MessageCopier extends App {
  private val config: Config = ConfigFactory.load()
  private val logger = LoggerFactory.getLogger(App.getClass)

  private val appConfig: Config = config.getConfig("copier")
  private val consumerConfig: Config = appConfig.getConfig("consumer")
  private val producerConfig: Config = appConfig.getConfig("producer")

  private val consumerConfigMap = new util.HashMap[String, Object]()

  consumerConfig.entrySet().forEach(
    a => {
      consumerConfigMap.put(a.getKey, a.getValue.unwrapped())
    }
  )

  private val producerConfigMap = new util.HashMap[String, Object]()

  producerConfig.entrySet().forEach(
    a => {
      producerConfigMap.put(a.getKey, a.getValue.unwrapped())
    }
  )


  private val consumer = new KafkaConsumer[String, User](consumerConfigMap)
  private val producer = new KafkaProducer[String, User](producerConfigMap)

  private val shuttingDown = new AtomicBoolean(false)

  val messagesProcessed = new AtomicLong(0)

  Exit.addShutdownHook("consumer shutdown hook", () => {
    shuttingDown.getAndSet(true)
    //TODO (KafkaConsumer is not safe for multi-threaded access. currentThread(name: consumer shutdown hook, id: 33))
    Utils.closeQuietly(producer, "producer close from shutdown hook thread")
  })


  try {
    runCopierLoop()
    Exit.exit(0)
  }
  catch {
    case e: Exception =>
      logger.error("Shutting down after unexpected error in event loop", e)
      System.err.println("Shutting down after unexpected error " + e.getClass.getSimpleName + ": " + e.getMessage + " (see the log for additional detail)")
      Exit.exit(1)
  }


  private def runCopierLoop(): Unit = {
    consumer.subscribe(consumerConfig.getStringList("topics"))
    producer.initTransactions()
    try {
      while (!shuttingDown.get()) {
        val records = consumer.poll(Duration.ofMillis(200))
        if (records.count() > 0) {
          try {
            producer.beginTransaction()

            records.forEach(record => {
              producer.send(consumerRecordToProducerRecord(producerConfig.getString("topic"), record), (metadata: RecordMetadata, exception: Exception) => {
                if (exception != null)
                  logger.error("Exception in call back {}", exception.getMessage)
                else {
                  RecordMetadataUtil.prettyPrinter(metadata)
                }
              })
            })
            producer.sendOffsetsToTransaction(defineNewOffsetsForTransaction(consumer), consumer.groupMetadata())

            producer.commitTransaction()
          }
          catch {
            case e: ProducerFencedException =>
              throw new KafkaException(String.format("The transactional.id %s has been claimed by another process", producerConfig.getString("transactional.id")), e);
            case e: KafkaException =>
              producer.abortTransaction()
              resetLastCommitedPosition(consumer)
          }

        }
      }
    } catch {
      case we: WakeupException =>
        if(!shuttingDown.get()) //Let the exception propagate if the exception was not raised as part of shutdown
          throw we
    }
    finally {
      Utils.closeQuietly(consumer, "consumer")
      Utils.closeQuietly(producer, "producer")
    }

  }

  def consumerRecordToProducerRecord(topic: String, consumerRecord: ConsumerRecord[String, User]): ProducerRecord[String, User] = {
    new ProducerRecord(topic, consumerRecord.partition, consumerRecord.key, consumerRecord.value)
  }

  def defineNewOffsetsForTransaction(consumer: KafkaConsumer[String, User]): JMap[TopicPartition, OffsetAndMetadata] = {
    val partitionToMetadata = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    consumer.assignment().stream().forEach(tp => {
      partitionToMetadata.put(tp, new OffsetAndMetadata(consumer.position(tp)))
    })
    partitionToMetadata
  }

  def messagesRemaining(kafkaConsumer: KafkaConsumer[String, User], topicPartition: TopicPartition): Long = {
    val currentPosition = consumer.position(topicPartition)
    val partitionToLong = consumer.endOffsets(Collections.singleton(topicPartition))
    if (partitionToLong.containsKey(topicPartition)) {
      return partitionToLong.get(topicPartition) - currentPosition
    }
    0
  }

  def resetLastCommitedPosition(kafkaConsumer: KafkaConsumer[String, User]): Unit = {
    val partitionToMetadata: JMap[TopicPartition, OffsetAndMetadata] = kafkaConsumer.committed(consumer.assignment())
    consumer.assignment().stream.forEach(tp => {
      val metadata = partitionToMetadata.get(tp)
      if (metadata != null) {
        consumer.seek(tp, metadata.offset())
      }
      else
        consumer.seekToBeginning(Collections.singleton(tp))
    })

  }

}
