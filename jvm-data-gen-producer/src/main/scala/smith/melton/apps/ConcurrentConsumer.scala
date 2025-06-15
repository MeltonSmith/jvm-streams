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
 * @author Melton Smith
 * @since 12.06.2025
 */
object ConcurrentConsumer {

  private val logger = LoggerFactory.getLogger(classOf[ConcurConsumerUserProcessor])

  private val config: Config = ConfigFactory.load("concurrentconsumer.conf")

  private val consumerConfigMap = new util.HashMap[String, Object]()

  private val concurrentConsumerUserProcessor = new ConcurConsumerUserProcessor(config.getConfig("concurrentConsumerUserProcessor"))

  private val shuttingDown = new AtomicBoolean(false)

  config.getConfig("consumer").entrySet().forEach(
    a => {
      consumerConfigMap.put(a.getKey, a.getValue.unwrapped())
    }
  )


  private val consumer = new KafkaConsumer[String, User](consumerConfigMap)
  consumer.subscribe(config.getConfig("consumer").getStringList("topics"))


  Exit.addShutdownHook("consumer shutdown hook", () => {
    shuttingDown.getAndSet(true)
    consumer.wakeup()
  })

  try {
    concurrentConsumerUserProcessor.start()
    runLoop()
    concurrentConsumerUserProcessor.close()
    Exit.exit(0)
  }
  catch {
    case e: Exception => {
      logger.error("Shutting down after unexpected error in event loop", e)
      System.err.println("Shutting down after unexpected error " + e.getClass.getSimpleName + ": " + e.getMessage + " (see the log for additional detail)")
      Exit.exit(1)
    }
  }

  private def runLoop(): Unit = {
    concurrentConsumerUserProcessor.start()
    concurrentConsumerUserProcessor.close()
    try {
      while (!shuttingDown.get()) {
        val value: ConsumerRecords[String, User] = consumer.poll(Duration.ofMillis(200))
        //TODO avoid at most once by waiting offsets per partition
        concurrentConsumerUserProcessor.processRecords(value)
        consumer.pause(concurrentConsumerUserProcessor.processingTasks.keySet())

        val partitionToMetadata = concurrentConsumerUserProcessor.getOffsets()

        consumer.commitSync(partitionToMetadata)
        consumer.resume(partitionToMetadata.keySet())
      }
    } catch {
      case e: WakeupException => {
        if (!shuttingDown.get())
          throw e;
      }
    }
    finally {
      Utils.closeQuietly(consumer, "Consumer closing")
    }


  }


}
