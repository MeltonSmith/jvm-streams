package smith.melton

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.{Exit, Utils}
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.slf4j.LoggerFactory
import smith.melton.faker.CustomResourceLoader.Implicits._
import smith.melton.faker.user.User
import smith.melton.util.RecordMetadataUtil

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.Random

/**
 * @author Melton Smith
 * @since 31.05.2025
 */
object App extends App {

  private val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS")
  private val config: Config = ConfigFactory.load()
  private val logger = LoggerFactory.getLogger(App.getClass)

  private val producerConfig: Config = config.getConfig("producer")

  private val mapFromSet = new util.HashMap[String, Object]()

  producerConfig.entrySet().forEach(
    a => {
      mapFromSet.put(a.getKey, a.getValue.unwrapped())
    }
  )

  private val totalMessageProcessed = new AtomicLong(0)

  private val producer = new KafkaProducer[String, User](mapFromSet)

  val isShuttingDown = new AtomicBoolean(false)

  Exit.addShutdownHook("producer-shutdown-hook", () => {
    isShuttingDown.set(true)
    Utils.closeQuietly(producer, "producer")
    System.out.println(statusAsJson("ShutdownComplete", totalMessageProcessed.get, 0, 0, "no tx"))
  })


  try {
    runLoop()
    Exit.exit(0)
  } catch {
    case e: Exception =>
      logger.error("Shutting down after unexpected error in event loop", e)
      System.err.println("Shutting down after unexpected error " + e.getClass.getSimpleName + ": " + e.getMessage + " (see the log for additional detail)")
      Exit.exit(1)
  }

  private def runLoop(): Unit = {
    var seed = Seed.apply(0)
    while (!isShuttingDown.get()) {
      val next = seed.next
      val user = Gen.oneOf(User.users).apply(Gen.Parameters.default, next).get
      producer.send(new ProducerRecord(producerConfig.getString("topic"), user.id.toString, user),
        (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null)
            logger.error("Exception in call back", exception)
          else {
            totalMessageProcessed.getAndAdd(1)
            RecordMetadataUtil.prettyPrinter(metadata)
          }
        })
//      if (Random.nextInt() % 3 == 0) {
//        throw new KafkaException("Manually on random")
//      }
      seed = next
      Thread.sleep(Random.nextLong(10000))
    }
  }


  private def statusAsJson(stage: String, totalProcessed: Long, consumedSinceLastRebalanced: Long, remaining: Long, transactionalId: String) = {
    toJsonString(Map[String, Any](
      "transactionalId" -> transactionalId,
      "stage" -> stage,
      "time" -> format.format(new Date()),
      "totalProcessed" -> totalProcessed,
    ))
  }

  private def toJsonString(data: Map[String, Any]) = {
    var json: String = null
    try {
      val mapper: JsonMapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        .build()
      json = mapper.writeValueAsString(data)
    } catch {
      case e: JsonProcessingException =>
        json = "Bad data can't be written as json: " + e.getMessage
    }
    json
  }

}


