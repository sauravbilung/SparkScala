package com.learning.spark._2_DataFramesAndDataSets

import org.apache.spark.sql._
import org.apache.log4j._

//In this example we are going to execute an actual SQL command on a dataset
// on Apache Spark across an entire cluster.
object _1_SparkSQLDataset {

  // case class is compact way of defining a class.
  // we have defined schema for our data.
  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate() //getOrCreate means create a session or get an existing one

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header", "true") // dataset has a header.
      .option("inferSchema", "true") // infer schema from the dataset.
      .csv("data/fakefriends.csv") // We have dataframe at this point. Schema here will be inferred at runtime.
      .as[Person] //At this point we are converting dataframe to dataset with explicit schema. This schema is known
      // at compile because of the person case class that we defined earlier.With explicit schema we get better
      // performance and better compile time error checking.

    //printing the schema of the dataset.
    schemaPeople.printSchema()

    // creating a database view on the data
    // We are creating a database table named "people" for our purpose.
    schemaPeople.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)

    //This is an important step. It is possible that the sessions will keep on running beyond the lifetime
    // of a driver script. If it is not stopped or is stopped unexpectedly. Then this sessions could still
    // be running on the cluster and getOrCreate() will reuse the existing session.
    spark.stop()
  }
}