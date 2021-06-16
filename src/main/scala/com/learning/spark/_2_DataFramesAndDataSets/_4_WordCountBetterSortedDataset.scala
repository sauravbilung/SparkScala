package com.learning.spark._2_DataFramesAndDataSets

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object _4_WordCountBetterSortedDataset {

  // No schema, every line will be called value.
  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    // Split using a regular expression that extracts words
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    // Normalize everything to lowercase
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy("word").count()

    // Sort by counts
    val wordCountsSorted = wordCounts.sort("count")

    // Show the results.
    // pass the number of rows that we want to show as parameter.
    wordCountsSorted.show(wordCountsSorted.count.toInt)


    // ANOTHER WAY TO DO IT (Blending RDD's and Datasets)
    // For analysis the dataset is always a better option because of SQL and
    // because of better optimisation features that are available.
    // Note we are getting the sparkContext out from spark session.
    val bookRDD = spark.sparkContext.textFile("data/book.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    val lowercaseWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountsDS = lowercaseWordsDS.groupBy("word").count()
    val wordCountsSortedDS = wordCountsDS.sort("count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)

  }
}

