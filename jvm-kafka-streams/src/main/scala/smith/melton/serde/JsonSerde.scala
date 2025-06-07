package smith.melton.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes
import smith.melton.agg.BalanceAggregator
import smith.melton.model.MoneyTransfer
import smith.melton.serde.JsonSerde.mapper

import scala.reflect.{ClassTag, classTag}

/**
 * @author Melton Smith
 * @since 02.06.2025
 */
object JsonSerde {
  val mapper = new ObjectMapper
  mapper.registerModule(DefaultScalaModule)

  implicit val moneyTransferSerde: Serde[MoneyTransfer] = Serdes.fromFn[MoneyTransfer](a => MoneyTransferSerde.serialize(a),
    a => MoneyTransferSerde.deserialize(a))

  implicit val balanceAggregatorSerde: Serde[BalanceAggregator] = Serdes.fromFn[BalanceAggregator](a => BalanceAggregator.serialize(a),
    a => BalanceAggregator.deserialize(a))
}

private object MoneyTransferSerde extends JsonSerde[MoneyTransfer]

private object BalanceAggregator extends JsonSerde[BalanceAggregator]

class JsonSerde[T >: Null : ClassTag]{

  def serialize(data: T): Array[Byte]  = {
    mapper.writeValueAsBytes(data)
  }

  def deserialize(data: Array[Byte]): Option[T] = {
    Option(mapper.readValue(data, classTag[T].runtimeClass.asInstanceOf[Class[T]]))
//    case null => null
//    case _ =>
//      try {
//
//      } catch {
//        case e: Exception =>
//          val jsonStr = new String(data, "UTF-8")
//          //            logger.warn(s"Failed parsing ${jsonStr}", e)
//          null
//      }
  }

}






