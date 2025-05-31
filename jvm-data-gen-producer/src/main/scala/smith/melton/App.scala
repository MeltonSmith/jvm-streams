package smith.melton

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.shaded.com.google.protobuf.Timestamp

import java.util
import java.util.Map.Entry
import java.util.function
import java.util.function.{BiConsumer, BinaryOperator, Supplier}
import java.util.stream.{Collector, Collectors}

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


  private val producer = new KafkaProducer(mapFromSet)


  private val infoes: util.List[PartitionInfo] = producer.partitionsFor("test-topic")



}
