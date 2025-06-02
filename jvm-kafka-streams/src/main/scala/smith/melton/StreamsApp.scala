package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.serialization.Serdes._
import smith.melton.model.MoneyTransfer
import smith.melton.serde.JsonSerde._

import java.util
import java.util.Properties

/**
 * @author Melton Smith
 * @since 02.06.2025
 */
object StreamsApp extends App {

  private val config: Config = ConfigFactory.load().getConfig("streams")

  private val mapFromSet = new util.HashMap[String, Object]()
  config.entrySet().forEach(
    a => {
      mapFromSet.put(a.getKey, a.getValue.unwrapped())
    }
  )



  private val builder = new StreamsBuilder()

  private val stream: KStream[String, MoneyTransfer] = builder.stream("transfers_topic")(Consumed.`with`[String, MoneyTransfer])
    .peek((a,b) => println(b))


  private val streams = new KafkaStreams(builder.build(),new StreamsConfig(mapFromSet))
  streams.cleanUp()
  streams.start()


  Exit.addShutdownHook("kafka streams app shut down hook", () => streams.close())

}
