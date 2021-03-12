package com.mohek.dp.streaming.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StructSocketStreaming {

    val spark = SparkSession.builder()
      .appName("Spark Streaming Hello")
      .master("local[2]")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def getSocketData(): Unit ={
    val lines = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port", "9999")
      .load()

    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val queryLines = wordCounts.writeStream
      .format("console")
      .outputMode("update")
      .start()

    queryLines.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    getSocketData()

  }

}
