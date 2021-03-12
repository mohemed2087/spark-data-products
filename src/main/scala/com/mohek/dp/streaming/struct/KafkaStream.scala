package com.mohek.dp.streaming.struct

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.mohek.dp.streaming._
import com.mohek.dp.streaming.sink._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.streaming.OutputMode

class KafkaStream {

  val spark: SparkSession = SparkSession.builder()
    .appName("Kafka Streaming")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  val host = "localhost"
  val clusterName = "cassandra"
  val keyspace = "stream"
  val tableName = "dept_count"

  def runStream(): Unit = {

    spark.setCassandraConf(clusterName, CassandraConnectorConf.ConnectionHostParam.option(host))

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events")
      .option("startingOffsets", "earliest")
      .load()


    val empDF = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", empStruct).as("employee")).select("employee.*")

    // val empDS = empDF.as[emp]

    val countDF = empDF.groupBy($"dep_id").count()


    val queryLines = countDF.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write       // Use Cassandra batch data source to write streaming out
          .cassandraFormat(tableName, keyspace)
          .option("cluster", clusterName)
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    queryLines.awaitTermination()


  }

}

object KafkaStream {

  def main(args: Array[String]): Unit = {

    new KafkaStream().runStream()
  }
}
