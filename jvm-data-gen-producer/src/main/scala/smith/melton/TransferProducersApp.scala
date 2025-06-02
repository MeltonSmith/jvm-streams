package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.utils.{Exit, Utils}
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.slf4j.LoggerFactory
import smith.melton.faker.user.User
import smith.melton.util.RecordMetadataUtil
import smith.melton.util.Util.statusAsJson
import smith.melton.faker.CustomResourceLoader.Implicits._
import smith.melton.model.MoneyTransfer

import java.text.SimpleDateFormat
import java.util
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.Random

/**
 * @author Melton Smith
 * @since 02.06.2025
 */
object TransferProducersApp  extends App {
  private val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS")
  private val config: Config = ConfigFactory.load()
  private val logger = LoggerFactory.getLogger(App.getClass)
  private val appKey = "transfers_producer_app"
  private val appCfg = config.getConfig(appKey)

  private val producerConfig: Config = appCfg.getConfig("producer")
  private val mapFromSet = new util.HashMap[String, Object]()

  producerConfig.entrySet().forEach(
    a => {
      mapFromSet.put(a.getKey, a.getValue.unwrapped())
    }
  )

  private val totalMessageProcessed = new AtomicLong(0)

  private val producer = new KafkaProducer[UUID, MoneyTransfer](mapFromSet)

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
      val userIdGen = Gen.oneOf(Range.apply(0, 13))

      val tpleGen = for {
        id1 <- userIdGen
        id2 <- userIdGen suchThat (_ != id1)
        amount <- Gen.choose(1, 20000)
      } yield (id1, id2, amount)

      //id2 is the destination
      val tple = tpleGen.apply(Gen.Parameters.default, next).get
      val uuid = Gen.uuid.apply(Gen.Parameters.default, next).get

      val record = new ProducerRecord(producerConfig.getString("topic"), uuid, MoneyTransfer(
        tple._1, tple._2, tple._3
      ))


      producer.send(record,
        (metadata: RecordMetadata, exception: Exception) => {
          if (exception != null)
            logger.error("Exception in call back", exception)
          else {
            totalMessageProcessed.getAndAdd(1)
            RecordMetadataUtil.prettyPrinter(metadata)
          }
        })
      seed = next
      Thread.sleep(Random.nextLong(10000))
    }
  }


}
