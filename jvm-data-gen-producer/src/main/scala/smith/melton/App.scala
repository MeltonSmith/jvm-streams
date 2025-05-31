package smith.melton

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.utils.{Exit, Utils}
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import smith.melton.faker.CustomResourceLoader.Implicits._
import smith.melton.faker.user.User
import smith.melton.util.RecordMetadataUtil

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

/**
 * @author Melton Smith
 * @since 31.05.2025
 */
object App extends App {


  private val FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS")
  private val config: Config = ConfigFactory.load()

  private val producerConfig: Config = config.getConfig("producer")

  private val mapFromSet = new util.HashMap[String, Object]()

  producerConfig.entrySet().forEach(
    a => {
      mapFromSet.put(a.getKey, a.getValue.unwrapped())
    }
  )

  val totalMessageProcessed = new AtomicLong(0)


  private val producer = new KafkaProducer[String, User](mapFromSet)

//  Runtime.getRuntime.addShutdownHook(new Thread(() => {
//    try {
//
////      logger.("Stream stopped")
//    } catch {
//      case exc: Exception =>
////        log.error("Got exception while executing shutdown hook: ", exc)
//    }
//
//  }))

  Exit.addShutdownHook("transactional-message-copier-shutdown-hook", () => {
    System.out.println(statusAsJson("ShutdownComplete", totalMessageProcessed.get, 0 , 0 ,"no tx"))

  })


  try {
    runLoop()
    Exit.exit(0)
  } catch {
    case e: Exception =>
      System.err.println("Shutting down after unexpected error " + e.getClass.getSimpleName + ": " + e.getMessage + " (see the log for additional detail)")
      Exit.exit(1)
  }

  private def runLoop(): Unit = {
    try{
      var seed = Seed.apply(0)
      while (true) {
        val next = seed.next
        val user = Gen.oneOf(User.users).apply(Gen.Parameters.default, next).get
        producer.send(new ProducerRecord(producerConfig.getString("topic"), user.id.toString, user),
          (metadata: RecordMetadata, exception: Exception) => {
            totalMessageProcessed.getAndAdd(1)
            RecordMetadataUtil.prettyPrinter(metadata)
          })
        seed = next
        Thread.sleep(Random.nextLong(10000))
      }
    }
    finally {
      println("closing")
      Utils.closeQuietly(producer, "producer")
    }
  }







//  producer.send(null, new Callback {
//    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
//
//      metadata.
//    }
//  })

  private def statusAsJson(stage: String, totalProcessed: Long, consumedSinceLastRebalanced: Long, remaining: Long, transactionalId: String) = {
    toJsonString(Map[String, Any](
      "transactionalId" -> transactionalId,
      "stage" -> stage,
      "time" -> FORMAT.format(new Date()),
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


