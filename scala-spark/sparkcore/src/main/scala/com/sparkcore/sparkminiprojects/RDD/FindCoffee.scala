package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.File

// Bir CSV dosyasından içerisinde kahve geçen kelimeleri (ürünleri) bulma

object FindCoffee {
  def main(args: Array[String]): Unit = {

    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("FindCoffee")
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
    val rddRetail: RDD[String] = sc.textFile(dataPath)
    println("RDD First 5:")
    rddRetail.take(5).foreach(println)

    // Remove Headers
    val rddRetailWithoutHeader = rddRetail.mapPartitionsWithIndex(
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    )
    println("RDD Without Header:")
    rddRetailWithoutHeader.take(5).foreach(println)

    // Birim miktarı 30'dan büyük olanları filtreleme
    val rddFiltered = rddRetailWithoutHeader.filter(x => x.split(";")(3).toInt > 30)

    println("Unit Product > 30:")
    rddFiltered.take(5).foreach(println)

    // İçerisinde Coffee Geçen Ürünler
    val rddCoffee = rddRetailWithoutHeader
      .filter(x => x.split(";")(2).contains("COFFEE"))

    println("Coffee In Products:")
    rddCoffee.take(5).foreach(println)

    def pairFilter(line: String):Boolean = {
      var Description: String = line.split(";")(2)
      var UnitPrice: Float = line.split(";")(5).trim.replace(",", ".").toFloat
      Description.contains("COFFEE") && UnitPrice > 20.0
    }

    println("Unit Price Bigger than 20 and contains COFFEE Products:")
    rddRetailWithoutHeader
      .filter(x => pairFilter(x))
      .take(20)
      .foreach(println)


  }

}
