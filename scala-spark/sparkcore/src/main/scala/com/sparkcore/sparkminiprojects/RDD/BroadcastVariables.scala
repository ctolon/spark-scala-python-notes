package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import scala.io.Source


object BroadcastVariables {
  def main(args: Array[String]): Unit = {

    // Set Log Level for LOG4J
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create Spark Session
    val conf = new SparkConf().setAppName("BroadcastVariables").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // Get Data Paths as String
    val data1: File = new File(getClass.getClassLoader.getResource("products.csv").getPath)
    val dataPath1: String = data1.toString
    println(s"Data Path: ${dataPath1}")

    val data2: File = new File(getClass.getClassLoader.getResource("order_items.csv").getPath)
    val dataPath2: String = data2.toString
    println(s"Data Path: ${dataPath2}")

    def loadProducts(): Map[Int, String] = {
      var productIdAndName: Map[Int, String] = Map()
      val source = Source.fromFile(dataPath1)
      val lines = source.getLines()
        .filter(x => (!(x.contains("productCategoryId"))))

      for (line <- lines) {
        val productId = line.split(",")(0).toInt
        val productName = line.split(",")(2)

        productIdAndName += (productId -> productName)
      }

      return productIdAndName
    }

    val broadcastProduct = sc.broadcast(loadProducts())

    val orderItemsRDD = sc.textFile(dataPath2)
      .filter(!_.contains("orderItemName"))


    def makeOrderItemsPairRDD(line: String): (Int, Float) = {
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2).toInt
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4).toFloat
      val orderItemProductPrice = line.split(",")(5)

      (orderItemProductId, orderItemSubTotal)
    }

    val orderItemPairRDD = orderItemsRDD.map(makeOrderItemsPairRDD)
    orderItemPairRDD.take(6).foreach(println(_))

    val sortedOrders = orderItemPairRDD.reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x => (x._2, x._1))
    //.take(10).foreach(println(_))

    val sortedOrdersWithProductName = sortedOrders.map(x => (broadcastProduct.value(x._1), x._2))
    sortedOrdersWithProductName.take(10).foreach(println(_))
  }
}
