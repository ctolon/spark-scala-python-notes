package com.sparkcore.sparkfundamentals.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object CreateRDDs {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    /*
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Create-RDDs")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

        val sc = spark.sparkContext
    */

    val conf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("Transformations")
      .setExecutorEnv("spark.executor.memory", "4g")
      .setExecutorEnv("spark.driver.memory", "4g")

    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("\nCreate RDDs From List: ")
    val rddFromList: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8)) // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0]
    rddFromList.take(3).foreach(println)

    println("\nCreate RDDs From Map: ")
    val mapData: List[(Int, String)] = Map(1 -> "Batuhan", 2 -> "Cevat", 3 -> "Tolon").toList
    val rddFromMap: RDD[(Int, String)] = sc.makeRDD(mapData)
    rddFromMap.take(3).foreach(println)

    println("\nrCreate RDDs From Tuple: ")
    val rddFromListTuple: RDD[Product] = sc.makeRDD(List((1, 2, 3), (4, 5), (6, 7, 8))) //  org.apache.spark.rdd.RDD[Product with Serializable]
    rddFromListTuple.take(3).foreach(println)

    println("\nCreate RDDs From Spark Context range Method: ")
    val rddByParallelizeCollection4: RDD[Long] = sc.range(10000L, 20000L, 100) //  org.apache.spark.rdd.RDD[Long]  MapPartitionsRDD[4]
    rddByParallelizeCollection4.take(3).foreach(println)

    // Get Data Path as String
    val data: File = new File(getClass.getClassLoader.getResource("OnlineRetail.csv").getPath)
    val dataPath: String = data.toString
    println(s"\nData Path: ${dataPath}")

    println("\nCreate RDDs From File: ")
    val rddFromTextFile: RDD[String] = sc.textFile(dataPath)
    rddFromTextFile.take(3).foreach(println)

    sc.stop()
  }
}
