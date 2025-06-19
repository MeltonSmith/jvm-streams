package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import smith.melton.streaming.listener.GracefulStopListener

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.jdk.CollectionConverters._

/**
 * @author Melton Smith
 * @since 17.06.2025
 */
object StreamingUserTxApp extends App {

  private val config: Config = ConfigFactory.load("usertxstreamingapp/app.conf")

  private val sparkStreamingApp: Config = config.getConfig("sparkStreamingApp")




  private val sparkConfigMap = sparkStreamingApp
    .getConfig("spark")
    .entrySet()
    .asScala
    .map(e => e.getKey -> e.getValue.unwrapped().toString)
    .toMap

  implicit val sparkSession: SparkSession = SparkSession.builder()
    .config(sparkConfigMap)
    .create()

  sparkSession.sparkContext.setLogLevel("INFO") //TODO log4j2 conf


  sparkSession.streams.addListener(new GracefulStopListener(sparkSession.streams))

  val userTxStreamingQueryConfig = sparkStreamingApp.getConfig("usertxstreamingqueryinput")
    .entrySet()
    .asScala
    .map(e => e.getKey -> e.getValue.unwrapped().toString)
    .toMap

//  sparkSession.read.load().foreachPartition()

  val transfers =  sparkSession
    .readStream
    .format("kafka")
    .options(userTxStreamingQueryConfig)
    .load()


  val transfersQuery =  transfers.writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime(Duration(20, TimeUnit.SECONDS)))
    .outputMode("append")
    .queryName("transfers")
    .start()


  transfersQuery.awaitTermination()
}
