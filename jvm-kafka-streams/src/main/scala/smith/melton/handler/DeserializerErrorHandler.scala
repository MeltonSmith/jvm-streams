package smith.melton.handler

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse
import org.apache.kafka.streams.errors.{DeserializationExceptionHandler, ErrorHandlerContext}
import org.slf4j.{Logger, LoggerFactory}

import java.util

/**
 * @author Melton Smith
 * @since 08.06.2025
 */
class DeserializerErrorHandler extends DeserializationExceptionHandler{

  private val logg: Logger = LoggerFactory.getLogger(this.getClass.getName)

  override def handle(context: ErrorHandlerContext, record: ConsumerRecord[Array[Byte], Array[Byte]], exception: Exception): DeserializationExceptionHandler.DeserializationHandlerResponse = {
    logg.error("Received a deserialize error for {} cause {}", record, exception)
    DeserializationHandlerResponse.CONTINUE
  }

  override def configure(configs: util.Map[String, _]): Unit = {
  }
}
