package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.streams.kstream.{JoinWindows, Named}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, Materialized, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.{KeyValueStore, QueryableStoreTypes, Stores}
import org.apache.kafka.streams.{AutoOffsetReset, KafkaStreams, StreamsConfig}
import smith.melton.model.MoneyTransfer
import smith.melton.serde.JsonSerde._

import java.util

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

  builder.stream("transfers_topic")(Consumed.`with`[String, MoneyTransfer]
                                                .withOffsetResetPolicy(AutoOffsetReset.earliest()))
    .groupBy((_, v) => v.toUserId)(Grouped.`with`[Long, MoneyTransfer])
    .aggregate(0L)((_,v,c) => {
       c + v.long
//    })((Materialized.as[Long, Long](Stores.inMemoryKeyValueStore("in mem"))))
    })(Materialized.`with`[Long, Long, ByteArrayKeyValueStore])
    .toStream(Named.as("default_store"))
    .peek((k, v) => {
      println(s"key $k, value $v")
      println(s"================")
    })
//    .count()(Materialized.as[String, Long](Stores.inMemoryKeyValueStore("in mem")))
    .to("output-1")(Produced.`with`[Long, Long])



  private val topology = builder.build()
  println(topology.describe())

  private val streams = new KafkaStreams(topology,new StreamsConfig(mapFromSet))
  streams.cleanUp()
  streams.start()


  Exit.addShutdownHook("kafka streams app shut down hook", () => streams.close())

}
