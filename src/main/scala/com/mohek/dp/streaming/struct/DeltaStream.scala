package com.mohek.dp.streaming.struct

import java.util.UUID

import com.mohek.dp.streaming.Streamer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import com.mohek.dp.streaming._

class DeltaStream extends Streamer {

  val spark: SparkSession = SparkSession.builder()
    .appName("delta-stream")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val deltaPath = "/tmp/loans_delta"

  val checkpointLocation =  "/tmp/temporary-" + UUID.randomUUID.toString

  override def run(): Unit = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events")
      .option("startingOffsets", "earliest")
      .load()

    val laonDF = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", loan).as("loan")).select("loan.*")

    val query = laonDF.writeStream.format("delta")
      .option("checkpointLocation", checkpointLocation)
    .option("mergeSchema", "true").outputMode("append")
    .start(deltaPath)

    query.awaitTermination()

    }
}

object DeltaStream{
  def main(args: Array[String]): Unit = {
    new DeltaStream().run()
  }
}
