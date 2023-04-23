package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.File


case class OrderItemProperties(orderItemName: Int,
                               orderItemOrderId: Int,
                               orderItemQuantity: Int,
                               orderItemSubTotal: Double,
                               orderItemProductPrice: Double)
case class OrderItemProductIdPropertiesPair(orderItemProductId: String,
                                            orderItemProperties: OrderItemProperties)

case class ProductProperties(productCategoryId: String,
                             productName: String,
                             productDescription: String,
                             productPrice: String,
                             productImage: String)
case class ProductIdPropertiesPair(ProductId: String,
                                   ProductProperties: ProductProperties)

object JoinProducts {

  def main(args: Array[String]): Unit = {
    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("JoinProducts")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    // Set Spark Context
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel(Level.ERROR.toString)

    // Get Data Paths as String
    val data1: File = new File(getClass.getClassLoader.getResource("products.csv").getPath)
    val dataPath1: String = data1.toString
    val data2: File = new File(getClass.getClassLoader.getResource("order_items.csv").getPath)
    val dataPath2: String = data2.toString
    println(s"Data Path1: ${dataPath1}")
    println(s"Data Path2: ${dataPath2}")

    val rddProducts = sc.textFile(dataPath1)
      .filter(!_.contains("productCategoryId"))

    rddProducts.take(5).foreach(println)

    val rddOrderItems = sc.textFile(dataPath2)
      .filter(!_.contains("orderItemName"))

    rddOrderItems.take(5).foreach(println)

    val productIdPropertiesR = rddProducts.map(line => {
      val fields = line.split(",")
      val productId = fields(0)
      val productCategoryId = fields(1)
      val productName = fields(2)
      val productDescription = fields(3)
      val productPrice = fields(4)
      val productImage = fields(5)
      ProductIdPropertiesPair(productId, ProductProperties(productCategoryId, productName, productDescription, productPrice, productImage))

    })

    val orderItemProductIdPropertiesR = rddOrderItems.map(line => {
      val fields = line.split(",")
      val orderItemName = fields(0).toInt
      val orderItemOrderId = fields(1).toInt
      val orderItemProductId = fields(2)
      val orderItemQuantity = fields(3).toInt
      val orderItemSubTotal = fields(4).toDouble
      val orderItemProductPrice = fields(5).toDouble
      if (fields.length != 6) {
        throw new RuntimeException("FATAL ERROR!")
      }
      OrderItemProductIdPropertiesPair(orderItemProductId, OrderItemProperties(orderItemName, orderItemOrderId, orderItemQuantity, orderItemSubTotal, orderItemProductPrice))
    })

    orderItemProductIdPropertiesR.map(x => (x.orderItemProductId, x.orderItemProperties))
      .take(5)
      .foreach(println)

    productIdPropertiesR.map(x => (x.ProductId, x.ProductProperties))
      .take(5)
      .foreach(println)

    val rddOrderItemsFinal = orderItemProductIdPropertiesR.map(x => (x.orderItemProductId, x.orderItemProperties))
    val rddProductsFinal = productIdPropertiesR.map(x => (x.ProductId, x.ProductProperties))

    val rddJoinedRelational = rddOrderItemsFinal.join(rddProductsFinal)
    println("\nRDD Joined Relational")
    rddJoinedRelational.take(10).foreach(println(_))

    println("orderItemsRDD satır sayısı: " + rddOrderItemsFinal.count())
    println("productsRDD satır sayısı: " + rddProductsFinal.count())
    println("orderItemProductJoinedRDD satır sayısı: " + rddJoinedRelational.count())

  }
}
