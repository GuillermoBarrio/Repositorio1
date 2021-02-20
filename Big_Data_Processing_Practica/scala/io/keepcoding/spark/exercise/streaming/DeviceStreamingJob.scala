package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}

import java.time.Duration
import scala.:+

object DeviceStreamingJob extends StreamingJob {



  // Creamos sesión de spark
  // Viene en soluciones local[20], pongo local[*]

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base - Streaming Devices")
    .getOrCreate()

  import spark.implicits._


  // Leemos desde kafka
  // El server y topic los pasamos como parámetros

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }


  // Parseamos lo leido desde kafka
  // El parámetro es el DF obtenido del def anterior
  // Hay que hacer un cast de timestamp, pq viene en formato Long, a TimestampType

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val deviceMessageSchema: StructType = ScalaReflection.schemaFor[DeviceMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json(col("value").cast(StringType), deviceMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
  }



  // Cambio nombre a readUsersMetadata
  // Leemos los datos ya en el postgres
  // Podemos hacer un read sin más, no es un streaming

  override def readUsersMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }



  // Enriquecemos los mensajes de los usuarios a traves de las antenas, con sus metadatos
  // Hacemos un join con el campo id de usuarios
  // Eliminamos (drop) uno de los campos id

  override def enrichDevicesWithMetadata(devicesDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    devicesDF.as("device")
      .withColumn("type", lit("a"))
      .join(
        metadataDF.as("metadata"),
        $"device.id" === $"metadata.id".cast(StringType)
      ).drop($"metadata.id")
      //.withColumn("type", lit("a"))
  }


  // El computerDevice lleva a cabo el withWatermark y el window
  // He tomado unos intervalo de tiempo menores para ver más facilmente si el programa funcionaba
  // Finalmente devuelve un DF ya listo para subir al Jdbc y al stoarge local

  override def computeDevicesStream(dataFrame: DataFrame, id: String, tipo: String): DataFrame = {
    dataFrame
      .withColumn("type", lit(tipo))
      .select($"timestamp", $"bytes", $"${id}", $"type")
      .withWatermark("timestamp", "30 seconds")
      .groupBy($"${id}", $"type", window($"timestamp", "1 minute").as("w"))
      .agg(sum($"bytes").as("value"))
      .select($"w.start".as("timestamp"), $"${id}".as("id"), $"value", $"type")

  }

  // El proceso de subida de los DF a Jdbc no presenta grandes novedades

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
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
      .start()
      .awaitTermination()
  }

  // Subimos los DF al storage de forma también habitual, pues los DF ya vienen con el formato correcto
  // Los parquets se ejecutan de forma habitual también

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}")
      .option("checkpointLocation", s"${storageRootPath}\\checkpoint3")
      .start()
      .awaitTermination()
  }


 // def main(args: Array[String]): Unit = run(args)

  // he preferido dejar los comentarios intermedios de como iba comprobando el avance del proyecto

  def main(args: Array[String]): Unit = {

    // Creamos un mapa (diccionario en scala) para combinar los id y types de la tabla bytes
    // Iremos iterando en él para hacer el compute, y la subida de datos al jdbc y al storage de forma más elegante

    val mapaCampos :Map[String, String] = Map.apply(
      "id" -> "id_bytes_total",
      "antenna_id" -> "antenna_bytes_total",
      "app" -> "app_bytes_total"
    )


    // Almacenamos en variables los datos de kafka

    val devicesDF = parserJsonData(readFromKafka("35.194.3.50:9092", "devices"))

    // Leemos los metadatos, con los enriqueceremos los procedentes de kafka

    val metadataDF = readUsersMetadata("jdbc:postgresql://35.238.42.105:5432/postgres", "user_metadata", "postgres", "password")



    val enrichDF = enrichDevicesWithMetadata(devicesDF, metadataDF)


    // Almacenamos los datos enriquecidos en el BATCH, de esa forma en el job de BATCH no tendremos que volver a hacer este JOIN
    // Ello crea los ficheros parquet

    val writeFuture = writeToStorage(enrichDF, "C:\\data")



    // Calculamos las agregaciones y las escribimos en Postgres


    val aggFutures = mapaCampos.map { case (id, tipo) => {
      println(tipo)

      writeToJdbc(
        computeDevicesStream(enrichDevicesWithMetadata(devicesDF, metadataDF), s"${id}", s"${tipo}"),
      "jdbc:postgresql://35.238.42.105:5432/postgres", "bytes", "postgres", "password")
      }
    }



    // Iteramos el mapa ahora para crear los ficheros parquet
    // Esta variable ya no la utilizamos, pero la conservo como muestra

    val writefutures = mapaCampos.map { case (id, tipo) => {
      println(tipo)

      writeToStorage(computeDevicesStream(enrichDevicesWithMetadata(devicesDF, metadataDF),
        s"${id}", s"${tipo}"), "C:\\data")

      }

    }

    // El await se aplica ahora a una secuencia, formada por los procesos de subida al Jdbc y la generación de los parquets

    Await.result(Future.sequence(Seq(writeFuture) ++ aggFutures), scala.concurrent.duration.Duration.Inf)



  }
}
