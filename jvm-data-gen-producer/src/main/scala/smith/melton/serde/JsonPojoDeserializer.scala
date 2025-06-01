package smith.melton.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import java.util

/**
 * @author Melton Smith
 * @since 01.06.2025
 */
class JsonPojoDeserializer[T] extends Deserializer[T] {

  val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()

  private var tClass: Class[T] = _

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    tClass = Class.forName(configs.get("entity.pojo.class").asInstanceOf[String]).asInstanceOf[Class[T]]
  }

  override def deserialize(topic: String, data: Array[Byte]): T = {
    if (data == null) {
      return null.asInstanceOf[T]
    }

    try {
      mapper.readValue(data, tClass)
    }
    catch {
      case e: Exception =>
        throw new SerializationException(e)
    }

  }
}
