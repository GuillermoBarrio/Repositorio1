package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  // Comento los def que no desarrollamos en el object adjunto

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  // def readUsersMetadataBatch(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  // def enrichDevicesParquetWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame


  def bytesByUser(dataFrame: DataFrame): DataFrame

  def bytesByAntenna(dataFrame: DataFrame): DataFrame

  def bytesByApp(dataFrame: DataFrame): DataFrame

  // def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame

  // def computePercentStatusByID(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    // val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    // val metadataDF = readUsersMetadataBatch(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    // val antennaMetadataDF = enrichDevicesParquetWithMetadata(antennaDF, metadataDF).cache()
    // val aggByCoordinatesDF = bytesByAntenna(antennaMetadataDF)
    // val aggPercentStatusDF = computePercentStatusByID(antennaMetadataDF)
    // val aggErroAntennaDF = computeErrorAntennaByModelAndVersion(antennaMetadataDF)

    // writeToJdbc(aggByCoordinatesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    // writeToJdbc(aggPercentStatusDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    // writeToJdbc(aggErroAntennaDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

    // writeToStorage(antennaDF, storagePath)

    spark.close()
  }

}
