package com.mohek.dp.batch

import org.apache.spark.sql.SparkSession

class DeltaBatch{

  val spark = SparkSession.builder()
    .appName("delta lake-batch")
    .master("local[2]")
    .getOrCreate()

  def run(): Unit ={

    val sourcePath = "/home/ubuntu/workspace/spark-data-products/src/test/resources/data/delta/loan-risks.snappy.parquet"
    val deltaPath = "/tmp/loans_delta"

    spark
      .read
      .format("parquet")
      .load(sourcePath)
      .write
      .format("delta")
      .save(deltaPath)

    spark
      .read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView("loans_delta")

    spark.sql("SELECT * FROM loans_delta LIMIT 5").show()

  }

}

object DeltaBatch {

  def main(args: Array[String]): Unit = {

    new DeltaBatch().run()

  }

}
