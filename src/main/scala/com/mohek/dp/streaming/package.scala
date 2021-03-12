package com.mohek.dp

import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}

package object streaming {

  val studentSchema= StructType(Array(
    StructField("id",IntegerType),
    StructField("name",StringType),
    StructField("age",IntegerType)
  ))

  val empSchema=StructType(Array(
    StructField("id",IntegerType),
    StructField("name",StringType),
    StructField("dep_id",IntegerType),
    StructField("salary",IntegerType)
  ))

  val empStruct = new StructType()
    .add("id", DataTypes.IntegerType)
    .add("name", DataTypes.StringType)
    .add("dep_id", DataTypes.IntegerType)
    .add("salary", DataTypes.IntegerType)

  val WindowWord = new StructType()
    .add("timestamp", DataTypes.TimestampType)
    .add("word", DataTypes.StringType)

  case class emp(id : Int, name : String, dep_id: Int, salary: Int)

  case class UserStatus(userId: String, active: Boolean)
  case class UserAction(userId: String, action: String)

}
trait Streamer{

  def run()

}
