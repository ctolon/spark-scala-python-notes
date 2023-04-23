package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File

object MapAndFlatMap {

  def main(args: Array[String]): Unit = {
    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("MapAndFlatMap")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    // Set Spark Context
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel(Level.ERROR.toString)

    // Get Data Path as String
    val data: File = new File(getClass.getClassLoader.getResource("simple_data.csv").getPath)
    val dataPath: String = data.toString
    println(s"Data Path: ${dataPath}")

    // Read csv file without header
    val humansRDD: RDD[String] = sc.textFile(dataPath)
      .filter(!_.contains("sirano"))

    println("\nOrginal RDD: ")
    humansRDD.take(3).foreach(println)
    println("\nmap() RDD: ")
    humansRDD
      .map(_.toUpperCase)
      .take(5).foreach(println)

    println("\nflatMap() RDD: ")
    humansRDD
      .flatMap(x => x.split(","))
      .map(x => x.toUpperCase)
      .take(15).foreach(println)

    sc.stop()
  }

}
