package com.sparkcore.sparkminiprojects.DataFrame

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.File

object DfFundamentals {

  def main(args: Array[String]): Unit = {
    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("DfFundamentals")
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

    import spark.implicits._
    val myList = List(1,2,3,4,5)
    val listRDD = sc.parallelize(myList)
      .toDF("numbers")

    listRDD.printSchema()

    val myList2 = spark.range(10,100,5).toDF("numbers")
    myList2.printSchema()

    val dfFromFile = spark.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dataPath)

    dfFromFile.printSchema()

    dfFromFile.show(10,truncate = false)

    // count() Action
    println("\nOnlineRetail satır sayısı: " + dfFromFile.count())


    dfFromFile.select("InvoiceNo", "Quantity") show (10)

    //dfFromFile.sort('Quantity).show(10)
    //dfFromFile.sort($"Quantity").show(10)
    dfFromFile.select("Quantity").sort(dfFromFile.col("Quantity")).show(10)

    // Dinamik conf ayarı ve shuffle partition sayısını değiştirme
    spark.conf.set("spark.sql.shuffle.partitions", "15")

    // explain(): Bu işi nasıl yapacağını bana bir anlat
    dfFromFile.sort($"Quantity").explain()


  }

}
