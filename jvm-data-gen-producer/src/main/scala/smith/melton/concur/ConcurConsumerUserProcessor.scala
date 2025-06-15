package smith.melton.concur

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import smith.melton.faker.user.User

import java.util.{HashMap => JHashMap}
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ConcurrentLinkedDeque, Executors}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{MILLISECONDS, SECONDS}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Success}

/**
 * @author Melton Smith
 * @since 12.06.2025
 */
class ConcurConsumerUserProcessor(config: Config) extends ConcurProcessor[String, User] {

  private val logger = LoggerFactory.getLogger(classOf[ConcurConsumerUserProcessor])
  private val processing = new AtomicBoolean(false)

  override val productQueue = new ArrayBlockingQueue[ConsumerRecords[String, User]](config.getInt("product-queue-size"))
  override val offsetQueue = new ConcurrentLinkedDeque[JMap[TopicPartition, OffsetAndMetadata]]()

  private implicit val workerPool: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  private val loopExecutor: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  val processingTasks = new ConcurrentHashMap[TopicPartition, Future[Long]]

  override def start(): Unit = {
    processing.set(true)
    Future(process())(loopExecutor)
  }

  override def processRecords(records: ConsumerRecords[String, User]): Unit = {
    try {
      logger.debug("Putting records into the process queue {}", records)
      productQueue.offer(records, 20, SECONDS)
    } catch {
      case _: InterruptedException => {
        Thread.currentThread().interrupt()
      }
    }
  }

  /**
   * Get the "ready" offsets to be commited per partition
   * @return
   */
  override def getOffsets(): JMap[TopicPartition, OffsetAndMetadata] = {
    offsetQueue.poll()
  }

  private def process(): Unit = {
    while (processing.get()) {
      try {
        val records = productQueue.poll()
        if (records != null) {
          records.partitions().forEach(partition => {
            records.records(partition).asScala.map(record => {
              processingTasks.put(partition, Future(processTask(record)))
            })

          })
        } else {
          logger.debug("No new records in the product queue")
        }
        checkActiveTasks()
      } catch {
        case _: InterruptedException => {
          processing.set(false)
          Thread.currentThread().interrupt()
        }
      }
    }
  }

  private def checkActiveTasks(): Unit = {
    val tpWithFinishedTasks = new ListBuffer[TopicPartition]
    val partitionToMetadata = new JHashMap[TopicPartition, OffsetAndMetadata]()
    processingTasks.forEach((k, v) => {
      if (v.isCompleted) {
        v.value match {
          case Some(value) => {
            value match {
              case Success(offset) => {
                partitionToMetadata.put(k, new OffsetAndMetadata(offset))
              }
              case Failure(exception) => {
                logger.error("Error while processing record, ex {}", exception)
              }
            }
          }
        }
      }
    })
    tpWithFinishedTasks.foreach(processingTasks.remove)
    logger.debug("Putting offsets into the queue")
    offsetQueue.offer(partitionToMetadata)
  }

  private def processTask(consumerRecord: ConsumerRecord[String, User]): Long = {
    val user = consumerRecord.value
    logger.info("Processed a record with key {}, user id {}, user name {}, in thread", consumerRecord.key, user.id, user.name, Thread.currentThread().getName)
    try
      //Simulate a long time to process each record
      Thread.sleep(Random.nextLong(10000))
    catch {
      case e: InterruptedException => {
        logger.info("interrupted in process task")
        Thread.currentThread.interrupt()
      }

    }
    consumerRecord.offset()
  }

  override def close(): Unit = {
    logger.info("Received signal to close for processor")
    processing.set(false)
    loopExecutor.awaitTermination(5000, MILLISECONDS)
    loopExecutor.shutdown()

    logger.debug("Shutting down inner caching working pool")
    workerPool.awaitTermination(5000, MILLISECONDS)
    workerPool.shutdown()
  }
}
