package com.mohek.dp.streaming.struct

import com.mohek.dp.streaming.Streamer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.mohek.dp.streaming._
import org.apache.spark.sql.streaming.OutputMode

class StatefulStreaming extends Streamer{

  val spark: SparkSession = SparkSession.builder()
    .appName("Kafka Streaming")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def run(): Unit = {

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events")
      .option("startingOffsets", "earliest")
      .load()


    val empDF = df.selectExpr("CAST(value AS STRING)").select(from_json($"value", empStruct).as("employee")).select("employee.*")

    val countDF = empDF.groupBy($"dep_id").count()

    val queryLines = countDF.writeStream
      .format("console")
      .outputMode("update")
      .start()

    queryLines.awaitTermination()

  }


  import org.apache.spark.sql.streaming._
  def updateUserStatus(userId: String, newActions: Iterator[UserAction], state: GroupState[UserStatus]): UserStatus = {

    val userStatus : UserStatus = state.getOption.getOrElse {
      UserStatus(userId, false)
    }
    newActions.foreach { action =>
    //  userStatus.updateWith(action)
    }
    state.update(userStatus)
    return userStatus
  }

}

object StatefulStreaming {

  def main(args: Array[String]): Unit = {

    new StatefulStreaming().run()

  }

}
