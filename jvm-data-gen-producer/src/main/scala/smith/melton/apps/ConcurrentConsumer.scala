package smith.melton.apps

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.utils.{Exit, Utils}
import org.slf4j.LoggerFactory
import smith.melton.concur.ConcurConsumerUserProcessor
import smith.melton.faker.user.User

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A consumer with a concurrent user processor backend
 *
 * @author Melton Smith
 * @since 12.06.2025
 */
object ConcurrentConsumer  extends App{

  private val logger = LoggerFactory.getLogger(classOf[ConcurConsumerUserProcessor])

  private val config: Config = ConfigFactory.load("concurrentconsumer.conf")

  private val consumerConfigMap = new util.HashMap[String, Object]()

  private val concurrentConsumerUserProcessor = new ConcurConsumerUserProcessor(config.getConfig("concurrentConsumerUserProcessor"))

  private val shuttingDown = new AtomicBoolean(false)

  private var lastCommitedMills = 0L

  config.getConfig("consumer").entrySet().forEach(
    a => {
      consumerConfigMap.put(a.getKey, a.getValue.unwrapped())
    }
  )

  private val consumer = new KafkaConsumer[String, User](consumerConfigMap)
  consumer.subscribe(config.getConfig("consumer").getStringList("topics"))


  Exit.addShutdownHook("consumer shutdown hook", () => {
    logger.error("Shut down hook started...")
    shuttingDown.getAndSet(true)
    consumer.wakeup()
    concurrentConsumerUserProcessor.close()
  })

  try {
    concurrentConsumerUserProcessor.start()
    runLoop()
    Exit.exit(0)
  }
  catch {
    case e: Exception => {
      logger.info("Shutting down after unexpected error in event loop", e)
      System.err.println("Shutting down after unexpected error " + e.getClass.getSimpleName + ": " + e.getMessage + " (see the log for additional detail)")
      Exit.exit(1)
    }
  }

  private def runLoop(): Unit = {

    try {
      while (!shuttingDown.get()) {
        val value: ConsumerRecords[String, User] = consumer.poll(Duration.ofMillis(200))
        //TODO avoid at most once by waiting offsets per partition
        concurrentConsumerUserProcessor.processRecords(value)
        val partitionsInProgress = concurrentConsumerUserProcessor.processingTasks.keySet()
        logger.debug("pause with {}", partitionsInProgress)
        consumer.pause(partitionsInProgress)

        val partitionToMetadata = concurrentConsumerUserProcessor.getOffsets()
        if (partitionToMetadata != null) {

          val interval = 500L
          try {
            val l = System.currentTimeMillis
            if (l - lastCommitedMills > interval) {
                 logger.debug("Commiting offsets {}", partitionToMetadata)
                consumer.commitSync(partitionToMetadata)
              lastCommitedMills = l
            }
          } catch {
            case _: Exception =>
              System.out.println("Failed to commit offsets")
          }
          val partitions = partitionToMetadata.keySet()
          logger.debug("resuming with partitions: {}", partitions)
          consumer.resume(partitions)
        }

      }
    } catch {
      case e: WakeupException => {
        if (!shuttingDown.get())
          throw e
      }
    }
    finally {
      Utils.closeQuietly(consumer, "Consumer closing")
    }


  }


}
