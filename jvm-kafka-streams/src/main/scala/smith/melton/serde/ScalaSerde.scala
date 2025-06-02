//package smith.melton.serde
//
//import org.apache.kafka.common.serialization.{Serde, Deserializer => JDeserializer, Serializer => JSerializer}
//
///**
// * Deprecated
// * @author Melton Smith
// * @since 02.06.2025
// */
//@Deprecated
//trait ScalaSerde[T] extends Serde[T] {
//  override def deserializer(): JDeserializer[T]
//
//  override def serializer(): JSerializer[T]
//
//  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
//
//  override def close(): Unit = ()
//}
//@Deprecated
//trait StatelessScalaSerde[T >: Null] extends Serde[T] with ScalaSerde [T]{
//
//  def serialize(data: T): Array[Byte]
//  def deserialize(data: Array[Byte]): Option[T]
//
//
//  override def serializer(): Serializer[T] = {
//    (data: T) => serialize(data)
//  }
//
//  override def deserializer(): Deserializer[T] = {
//    (data: Array[Byte]) => deserialize(data)
//  }
//}
//@Deprecated
//trait Deserializer[T >: Null] extends JDeserializer[T] {
//  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
//
//  override def close(): Unit = ()
//
//  override def deserialize(topic: String, data: Array[Byte]): T =
//    Option(data).flatMap(deserialize).orNull
//
//  def deserialize(data: Array[Byte]): Option[T]
//}
//@Deprecated
//trait Serializer[T] extends JSerializer[T] {
//  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
//
//  override def close(): Unit = ()
//
//  override def serialize(topic: String, data: T): Array[Byte] =
//    Option(data).map(serialize).orNull
//
//  def serialize(data: T): Array[Byte]
//}
