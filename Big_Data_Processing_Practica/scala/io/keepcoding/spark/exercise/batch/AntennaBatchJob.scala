package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AntennaBatchJob extends BatchJob {



  // Abrimos sesión sin novedad

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._



  // Leemos del storage los ficheros paquet

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

/*

  override def readUsersMetadataBatch(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }


  override def enrichDevicesParquetWithMetadata(DevicesDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    DevicesDF.as("devices")
      .join(
        metadataDF.as("metadata"),
        $"devices.id" === $"metadata.id"
      ).drop($"metadata.id")

  }

*/



  // Sumamos el consumo de bytes por usuario en la ventana de una hora
  // Devolvemos ya el formato de DF para que luego podamos hacer los select de consumos por email, y exceso de quota

  override def bytesByUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes", $"type", $"email", $"quota")
      .withColumn("type", lit("id_bytes_total"))
      .groupBy($"email", $"type", $"quota", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value"),
      )
      .select($"window.start".as("timestamp"), $"email".as("id"), $"value", $"type", $"quota")
  }



  // Calculamos los bytes transmitidos por antena en la ventana de una hora
  // Devolvemos ya el formato de DF adecuado para subirlo luego al Jdbc

  override def bytesByAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      //.filter($"type" === lit("antenna_bytes_total"))
      .select($"timestamp", $"antenna_id", $"bytes", $"type")
      .withColumn("type", lit("antenna_bytes_total"))
      .groupBy($"antenna_id", $"type", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value"),
      )
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value", $"type")
  }



  // Calculamos los bytes que pasan a través de cada app en la ventana de una hora
  // Devolvemos ya el formato de DF adecuado para subirlo luego al Jdbc

  override def bytesByApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      //.filter($"type" === lit("app_bytes_total"))
      .select($"timestamp", $"app", $"bytes", $"type")
      .withColumn("type", lit("app_bytes_total"))
      .groupBy($"app", $"type", window($"timestamp", "1 hour"))
      .agg(
        sum($"bytes").as("value"),
      )
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value", $"type")
  }



  // Ejecutamos la subida de datos al Jdbc de forma habitual

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }


  // No es necesario el def write to storage, aunque no lo comentamos por si alguna vez fuese necesario

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"${storageRootPath}/historical")
  }

  // def main(args: Array[String]): Unit = run(args)

  // Definimos los pasos y variables del mail desde aquí

  def main(args: Array[String]): Unit = {

    // El argsTime

    val argsTime = "2021-02-19T20:00:00Z"


    // Leemos desde e storage los ficheros parquet y los almacenamos en el cache

    val devicesDF = readFromStorage("C:\\data", OffsetDateTime.parse(argsTime)).cache()




    // DF de los bytes que transmiten las antenas

    val bytesByAntennaHour = bytesByAntenna(devicesDF)



    // DF de los bytes gestionados por cada app

    val bytesByAppHour = bytesByApp(devicesDF)



    // DF de los bytes consumidos por cada user
    // Aplicamos un select para dejarlo en el formato requerido por la tabla bytes_hourly de Jdbc

    val bytesByUserHour = bytesByUser(devicesDF)
      .select($"timestamp", $"id", $"value", $"type")





    // Almacenamos las tres DF resumen en una sequencia

    val seq: Seq[DataFrame] = Seq(bytesByAntennaHour, bytesByAppHour, bytesByUserHour)


    // Subimos a la tabla bytes_hourly Jdbc los tres DF resumen mediante un foreach

    seq.foreach{(item:DataFrame) =>  writeToJdbc(item, "jdbc:postgresql://35.238.42.105:5432/postgres", "bytes_hourly", "postgres", "password") }




    // Partimos del DF bytesByUser para filtrar aquellos users que han superado su quota horaria
    // Cambiamos de nombre las columnas necesarias para que coincidancon la base de datos del Jdbc
    // Hacemos el select de salida para que esté listo para subirlo al Jdbc

    val excessUsage = bytesByUser(devicesDF)
      .filter($"value" > $"quota")
      .withColumnRenamed("value", "usage")
      .withColumnRenamed("id", "email")
      .select("email", "usage", "quota", "timestamp")



    // Subimos los datos de los excesos a la tabla user_quota_limit de Jdbc

    writeToJdbc(excessUsage, "jdbc:postgresql://35.238.42.105:5432/postgres", "user_quota_limit", "postgres", "password")


  }
}
