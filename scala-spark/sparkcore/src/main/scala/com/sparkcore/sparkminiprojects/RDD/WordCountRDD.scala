package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File


object WordCountRDD {
  def main(args: Array[String]): Unit = {

    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("WordCountRdd")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    // Set Spark Context
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel(Level.ERROR.toString)

    // Get Data Path as String
    val data: File = new File(getClass.getClassLoader.getResource("omer_seyfettin_forsa_hikaye.txt").getPath)
    val dataPath: String = data.toString
    println(s"Data Path: ${dataPath}")

    // Count RDD
    val storyRDD: RDD[String] = sc.textFile(dataPath)
    println(s"RDD Count: ${storyRDD.count()}")

    // Split Words with Spaces
    val words: RDD[String] = storyRDD.flatMap(line => line.split(" "))

    // Create Tuple for each words as (word, 1) then apply reducedByKey
    val wordNumbers: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey((x, y) => x + y)
    println(s"Total Words: ${wordNumbers.count()}")

    // Take 10 (word,1) pair and print for checking
    println("Some Word Numbers Before Reversing:")
    wordNumbers.take(10).foreach(println)

    // Reverse (word,1) pairs as (1,word) and print for checking
    val wordNumbersReversed: RDD[(Int, String)] = wordNumbers.map(x => (x._2, x._1))
    wordNumbersReversed.take(15).foreach(println)

    println("**************   MOST DUPLICATED WORDS   ***************************")
    wordNumbersReversed.sortByKey(ascending = false).take(20).foreach(println)

    sc.stop()

  }

}
