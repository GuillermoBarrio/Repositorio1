package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}


// Cambia el schema de los mensajes de devices respecto a los de las antenas

case class DeviceMessage(bytes: Long, timestamp: Long, app: String, id: String, antenna_id: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def readUsersMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDevicesWithMetadata(devicesDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeDevicesStream(dataFrame: DataFrame, id: String, tipo: String): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val antennaDF = parserJsonData(kafkaDF)
    val metadataDF = readUsersMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichDevicesWithMetadata(antennaDF, metadataDF)
    val storageFuture = writeToStorage(antennaDF, storagePath)
   // val aggByCoordinatesDF = computeDevicesStream(antennaMetadataDF, id, tipo)
    // val aggFuture = writeToJdbc(aggByCoordinatesDF, id, tipo, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

   // Await.result(Future.sequence(Seq(aggFuture, storageFuture)), Duration.Inf)

    spark.close()
  }

}
