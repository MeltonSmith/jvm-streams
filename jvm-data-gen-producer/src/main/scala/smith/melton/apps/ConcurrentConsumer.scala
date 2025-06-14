package smith.melton.apps

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.utils.Exit
import smith.melton.MessageCopier.{consumer, consumerConfig}
import smith.melton.concur.ConcurConsumerUserProcessor
import smith.melton.faker.user.User

import java.time.Duration
import java.util
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

/**
 * @author Melton Smith
 * @since 12.06.2025
 */
object ConcurrentConsumer {

  private val config: Config = ConfigFactory.load("concurrentconsumer.conf")

  private val consumerConfigMap = new util.HashMap[String, Object]()

  private val concurrentConsumerUserProcessor = new ConcurConsumerUserProcessor(config.getConfig("concurrentConsumerUserProcessor"))


  config.getConfig("consumer").entrySet().forEach(
    a => {
      consumerConfigMap.put(a.getKey, a.getValue.unwrapped())
    }
  )


  private val consumer = new KafkaConsumer[String, User](consumerConfigMap)
  consumer.subscribe(config.getConfig("consumer").getStringList("topics"))

  try {
    concurrentConsumerUserProcessor.start()
    runLoop()
    concurrentConsumerUserProcessor.close()
    Exit.exit(0)
  }
  catch {
    case e: Exception => {

    }
  }


  def runLoop(): Unit = {
    try {
      val value: ConsumerRecords[String, User] = consumer.poll(Duration.ofMillis(200))
      //TODO avoid at most once by waiting offsets per partition
      concurrentConsumerUserProcessor.processRecords(value)

      consumer.pause(concurrentConsumerUserProcessor.processingTasks)
      consumer


    }

  }

  def committedOffsets()


}
