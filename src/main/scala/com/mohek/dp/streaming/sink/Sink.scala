package com.mohek.dp.streaming.sink

import org.apache.spark.sql.DataFrame

class Sink {

  def writeToCassandra(data : DataFrame):  Unit ={

    val queryLines = data.writeStream
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "stream")
      .option("table", "emp")
      .option("checkpointLocation","/home/ubuntu/data/stream/cpt")
      .outputMode("complete")
      .start()

    queryLines.awaitTermination()
  }

  def writeToConsole(data : DataFrame): Unit ={

    val queryLines = data.writeStream
      .format("console")
      .option("format","append")
      // .option("path","/home/ubuntu/data/stream/output")
      .option("checkpointLocation","/home/ubuntu/data/stream/cpt")
      .outputMode("complete")
      .start()

    queryLines.awaitTermination()

  }

  def writeToFile(data: DataFrame): Unit = {

    val queryLines = data.writeStream
      .format("json")
      .option("format","append")
      .option("path","/home/ubuntu/data/stream/output")
      .option("checkpointLocation","/home/ubuntu/data/stream/cpt")
      .outputMode("complete")
      .start()

    queryLines.awaitTermination()

  }
}
