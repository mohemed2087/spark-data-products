package com.mohek.dp.streaming.struct

import org.apache.spark.sql.SparkSession
import com.mohek.dp.streaming._

object FolderStreaming {

  val spark = SparkSession.builder()
    .appName("Folder Streaming Example Demo")
    .master("local[2]")
    .getOrCreate()

  def readFolderData() ={
    val df = spark.readStream
      .format("csv")
      .option("header" , false)
      .schema(studentSchema)
      .load("/home/ubuntu/workspace/spark-data-products/src/test/resporces/data/stream_input")

    df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  }

  def main(args: Array[String]): Unit = {
    readFolderData()
  }

}
