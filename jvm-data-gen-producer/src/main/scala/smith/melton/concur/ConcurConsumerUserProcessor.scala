package smith.melton.concur

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import smith.melton.faker.user.User

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentLinkedDeque, Executors}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{MILLISECONDS, SECONDS}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Random, Success}

/**
 * @author Melton Smith
 * @since 12.06.2025
 */
class ConcurConsumerUserProcessor(config: Config) extends ConcurProcessor[String, User]{

  private val logger = LoggerFactory.getLogger(classOf[ConcurConsumerUserProcessor])
  private val processing = new AtomicBoolean(false)

  override val productQueue = new ArrayBlockingQueue[ConsumerRecords[String, User]](config.getInt("TODO"))
  override val offsetQueue = new ConcurrentLinkedDeque[Map[TopicPartition, OffsetAndMetadata]]()

  private implicit val workerPool: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
  private val loopExecutor: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  val processingTasks = new scala.collection.mutable.HashMap[TopicPartition, Future[Long]]
  val offsetsToCommit = new scala.collection.mutable.HashMap[TopicPartition, OffsetAndMetadata]

  override def start(): Unit = {
    processing.set(true)
    Future(process())
  }

  override def processRecords(records: ConsumerRecords[String, User]): Unit = {
    try {
      logger.debug("Putting records into the process queue {}", records)
      productQueue.offer(records, 20, SECONDS)
    } catch {
      case e: InterruptedException => {
        Thread.currentThread().interrupt()
      }
    }
  }

  override def getOffsets(): Map[TopicPartition, OffsetAndMetadata] = {
    offsetsToCommit.toMap
  }

  def process(): Unit = {
    while (processing.get()) {
      try {
        val records = productQueue.poll()
        if (records != null) {
          records.partitions().forEach(partition => {
             records.records(partition).asScala.map(record => {
              processingTasks.put(partition, Future(processTask(record)))
            })

          })
        }
        checkActiveTasks()
      } catch {
        case
      }
    }
  }

  private def checkActiveTasks(): Unit = {
    val tpWithFinishedTasks = new ListBuffer[TopicPartition]
    processingTasks.foreach((tp: TopicPartition, consumerTask: Future[Long]) => {
      if (consumerTask.isCompleted) {
        tpWithFinishedTasks.addOne(tp)
        consumerTask.value match {
          case Some(value) => {
            value match {
              case Success(offset) => {
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset))
              }
              case Failure(exception) => {
                  logger.error("Error while processing record {}", )
              }
            }
          }
          //cant be
          case None => _
        }
      }

    })
    tpWithFinishedTasks.foreach(processingTasks.remove)
  }

  private def processTask(consumerRecord: ConsumerRecord[String, User]): Long = {
    val user = consumerRecord.value
    logger.info("Processed a record with {}, user id {}, user name {}", consumerRecord.key, user.id, user.name)
    try
      //Simulate a long time to process each record
      Thread.sleep(Random.nextLong(2000))
    catch {
      case e: InterruptedException =>
        Thread.currentThread.interrupt()
    }
    consumerRecord.offset()
  }

  override def close(): Unit = {
    logger.info("Received signal to close");
    processing.set(false)
    workerPool.awaitTermination(5000, MILLISECONDS)
    workerPool.shutdown()
  }
}
