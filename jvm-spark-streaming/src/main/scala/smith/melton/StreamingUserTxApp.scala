package smith.melton

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

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

  sparkSession.sparkContext.setLogLevel("ERROR")

  val userTxStreamingQueryConfig = sparkStreamingApp.getConfig("usertxstreamingquery")
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
//    .foreach()
    .format("console")
    .outputMode("append")
    .queryName("transfers")
    .start()


  transfersQuery.stop()
  transfersQuery.awaitTermination()
}
