package com.mohek.dp.streaming.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.mohek.dp.streaming._

class KafkaStreamWindowing {

  val spark = SparkSession.builder()
    .appName("Kafka Window Streaming")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def getKafkaStream(): Unit = {

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events")
      .option("startingOffsets", "earliest")
      .load()

    val words = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", WindowWord).as("words")).select("words.*")

    val windowedCounts = words.withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        $"word"
      ).count()

    val queryLines = windowedCounts.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("update")
      .start()

    queryLines.awaitTermination()

    spark.stop()

  }
}
object KafkaStreamWindowing {

  def main(args: Array[String]): Unit = {

    new KafkaStreamWindowing().getKafkaStream()
  }
  }



