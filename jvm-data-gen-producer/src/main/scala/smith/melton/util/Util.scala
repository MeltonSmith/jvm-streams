package smith.melton.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.consumer.ConsumerRecords
import sbt.testing.Task

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * @author Melton Smith
 * @since 01.06.2025
 */
object Util {

  private val format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS")

  def statusAsJson(stage: String, totalProcessed: Long, consumedSinceLastRebalanced: Long, remaining: Long, transactionalId: String) = {
    toJsonString(Map[String, Any](
      "transactionalId" -> transactionalId,
      "stage" -> stage,
      "time" -> format.format(new Date()),
      "totalProcessed" -> totalProcessed,
    ))
  }

  def toJsonString(data: Map[String, Any]) = {
    var json: String = null
    try {
      val mapper: JsonMapper = JsonMapper.builder()
        .addModule(DefaultScalaModule)
        .build()
      json = mapper.writeValueAsString(data)
    } catch {
      case e: JsonProcessingException =>
        json = "Bad data can't be written as json: " + e.getMessage
    }
    json
  }

}
