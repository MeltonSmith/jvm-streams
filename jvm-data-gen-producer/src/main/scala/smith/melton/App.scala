package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.PartitionInfo

import java.util
import org.scalacheck.{Arbitrary, Gen}
import smith.melton.faker.user.User
import smith.melton.faker.CustomResourceLoader.Implicits._
import org.scalacheck.rng.Seed
/**
 * @author Melton Smith
 * @since 31.05.2025
 */
object App extends App {


  private val config: Config = ConfigFactory.load()

  private val producerConfig: Config = config.getConfig("producer")

  private val mapFromSet = new util.HashMap[String, Object]()

  producerConfig.entrySet().forEach(
    a => {
      mapFromSet.put(a.getKey, a.getValue.unwrapped())
    }
  )

  private val maybeUser: Option[User] = Gen.oneOf(User.users).apply(Gen.Parameters.default, Seed.apply(0))





  private val producer = new KafkaProducer(mapFromSet)


  private val infoes: util.List[PartitionInfo] = producer.partitionsFor("test-topic")

//  producer.send(null, new Callback {
//    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
//
//      metadata.
//    }
//  })

}
