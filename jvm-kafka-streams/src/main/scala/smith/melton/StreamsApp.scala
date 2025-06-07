package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.streams.kstream.{Named, Printed}
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes
import org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder, kstream}
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.internals.RocksDBStore
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, Stores}
import org.apache.kafka.streams.{AutoOffsetReset, KafkaStreams, StreamsConfig}
import smith.melton.agg.BalanceAggregator
import smith.melton.model.MoneyTransfer
import smith.melton.serde.JsonSerde._

import java.time.Duration.ofMinutes
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

//    builder.stream("transfers_topic")(Consumed.`with`[String, MoneyTransfer]
//                                                  .withOffsetResetPolicy(AutoOffsetReset.earliest()))
//      .groupBy((_, v) => v.toUserId.toString)(Grouped.`with`[String, MoneyTransfer])
//      .aggregate(0L)((_,v,c) => {
//         c + v.amount
//      })(Materialized.as[String, Long, ByteArrayKeyValueStore]("groupedTransfersAsKStream")(Serdes.stringSerde, Serdes.longSerde))
//      .mapValues(v => v.toString)
//  //    .suppress(untilTimeLimit(ofMinutes(5), maxBytes(1_000_000L).emitEarlyWhenFull()))
//      .toStream(Named.as("default_store"))
//      .peek((k, v) => {
//        println(s"key $k, value $v")
//        println(s"================")
//      })
  //    .count()(Materialized.as[String, Long](Stores.inMemoryKeyValueStore("in mem")))
//      .to("output-1")(Produced.`with`[String, String])
  implicit val a: Materialized[Long, BalanceAggregator, ByteArrayKeyValueStore] = Materialized.`with`[Long, BalanceAggregator, ByteArrayKeyValueStore]
  implicit val b: KeyValueBytesStoreSupplier = (Stores.inMemoryKeyValueStore("in mem"))

  private val function: (Long, MoneyTransfer, BalanceAggregator) => BalanceAggregator = (_: Long, v: MoneyTransfer, agg: BalanceAggregator) => {
    agg.add(v.amount)
    agg
  }
  private val function1: (Long, MoneyTransfer, BalanceAggregator) => BalanceAggregator = (_: Long, v: MoneyTransfer, agg: BalanceAggregator) => {
    agg.substract(v.amount)
    agg
  }
  private val aggregator: BalanceAggregator = new BalanceAggregator()


  private val value = builder.table("transfers_topic")(Consumed.`with`[String, MoneyTransfer]
      .withOffsetResetPolicy(AutoOffsetReset.latest()))
    .groupBy[Long, Long]((_, v) => (v.fromUserId, v.amount))(Grouped.`with`[Long, Long])
    .aggregate(0L)((k,v,c) => c + v, (k,v,c) => c - v)(Materialized.as[Long, Long, ByteArrayKeyValueStore]("newVer"))
//    .aggregate(aggregator)(function, function1)(Materialized.`with`[Long, BalanceAggregator, ByteArrayKeyValueStore](longSerde, balanceAggregatorSerde))
.toStream
//    .peek((k: Long, v: Long) => {
//          println(s"key $k, value $v")
//          println(s"================")
//        })
      .map((k,v) => (k.toString, v.toString))
      .to("output-balance-per-user")(Produced.`with`[String, String])


  private val topology = builder.build()
  println(topology.describe())

  private val streams = new KafkaStreams(topology, new StreamsConfig(mapFromSet))
  streams.cleanUp()
  //  streams.streamsMetadataForStore().forEach(a => {
  //    a.
  //  })
  streams.start()


  Exit.addShutdownHook("kafka streams app shut down hook", () => streams.close())

}
