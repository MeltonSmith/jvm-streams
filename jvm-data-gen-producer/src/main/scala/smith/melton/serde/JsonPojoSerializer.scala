package smith.melton.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Serializer

/**
 * @author Melton Smith
 * @since 01.06.2025
 */
class JsonPojoSerializer[T] extends Serializer[T] {

  val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()


  override def serialize(topic: String, data: T): Array[Byte] = {
    if (data == null)
      return null

    try mapper.writeValueAsBytes(data)
    catch {
      case e: Exception =>
        throw new SerializationException("Error serializing JSON message", e)
    }
  }
}
