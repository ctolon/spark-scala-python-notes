package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.File

case class CancelledPrice(isCancelled: Boolean, total: Double)
object CancelledSales {

  def main(args: Array[String]): Unit = {

    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("CancelledSales")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    // Set Spark Context
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel(Level.ERROR.toString)

    // Get Data Path as String
    val data: File = new File(getClass.getClassLoader.getResource("OnlineRetail.csv").getPath)
    val dataPath: String = data.toString
    println(s"Data Path: ${dataPath}")

    // Read RDD
    val retailRDD = sc.textFile(dataPath)
      .filter(!_.contains("InvoiceNo"))

    // Check Header is filtered
    println("Check:")
    println(retailRDD.first())

    val retailTotal = retailRDD.map(x => {
      val fields = x.split(";")
      val isCancelled: Boolean = fields(0).startsWith("C")
      val quantity: Double = fields(3).toDouble
      val price: Double = fields(5).replace(",", ".").toDouble
      val total: Double = quantity * price
      CancelledPrice(isCancelled, total)

    })

    // Check is work or not function case class
    retailTotal.take(5).foreach(println)

    // List Total Cancelled Products cost
    retailTotal.map(x => (x.isCancelled, x.total))
      .reduceByKey((x, y) => x + y)
      .filter(x => x._1)
      .map(x => x._2)
      .take(5)
      .foreach(println)

    // Alternative (with low verb. but not readable)
    val totalPrice: Double = retailTotal.filter(_.isCancelled)
      .map(_.total)
      .reduce(_ + _)

    print(totalPrice)

  }
}
