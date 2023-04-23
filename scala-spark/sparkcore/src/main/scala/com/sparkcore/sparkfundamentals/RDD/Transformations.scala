package com.sparkcore.sparkfundamentals.RDD

import breeze.numerics.pow
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Transformations {
  def main(args: Array[String]): Unit = {

    // Create Spark Session
    val spark: SparkSession = SparkSession.builder
      .master(master = "local[*]")
      .appName(name = "RDD Transformations")
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "4g")
      .getOrCreate()

    // Set sparkContext and set Log Level to ERROR
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel(Level.ERROR.toString)

    //////////////////////////////////
    // 1. RDD Basic Transformations //
    //////////////////////////////////

    // Create RDD From List with parallelize
    val myList: List[Int] = List(1, 2, 3, 4, 5, 6, 7)
    val listRDD: RDD[Int] = sc.parallelize(myList)
    println("My List:")
    listRDD
      .take(5)
      .foreach(x => print(s"${x} "))

    // Using map()
    println("\nMy List with Pow 3:")
    listRDD
      .map(x => pow(x, 3))
      .take(num = 5)
      .foreach(x => print(s"${x} "))

    // Using filter()
    println("\nMy List With Filter (element equals to 5):")
    val listRDDFiltered: RDD[Int] = listRDD.filter(x => x == 5)
    listRDDFiltered
      .take(num = 5)
      .foreach(x => print(s"${x} "))

    // Using flatMap()
    val txt: List[String] = List("Hello", "How are you?", "CERN-LHC", "Data Engineering")
    val txtRDD: RDD[String] = sc.parallelize(txt)
    val txtRDDUpper: Array[String] = txtRDD
      .flatMap(x => x.split(" "))
      .map(x => x.toUpperCase())
      .take(9)

    println("\nMy List With flatMap (every element in Upper Case):")
    txtRDDUpper.foreach(x => print(s"${x} "))

    // Using distinct()
    val myList2: List[Int] = List(2, 3, 4, 4, 5, 6, 2, 3, 64, 32, 2, 6, 8, 8)
    val myList2RDD: RDD[Int] = sc.parallelize(myList2)
    println("\nMy List With Distinct (un-duplicated elements) :")
    myList2RDD
      .distinct()
      .take(10)
      .foreach(x => print(s"${x} "))

    // Using sample()
    val mySamples = myList2RDD.sample(withReplacement = true, fraction = 0.7, seed = 45).take(10)
    println("\nMy List With Samples :")
    mySamples.foreach(x => print(s"${x} "))

    /////////////////////////////////
    // 2. RDD Pair Transformations //
    /////////////////////////////////

    val myList3: List[Int] = List(1, 5, 7, 9, 5, 7)
    val myList4: List[Int] = List(1, 4, 8, 19)

    val myList3RDD: RDD[Int] = sc.makeRDD(myList3)
    val myList4RDD: RDD[Int] = sc.makeRDD(myList4)

    // Using union()
    println("\nMy Lists With union (Birleşim) :")
    myList3RDD
      .union(myList4RDD)
      .take(10)
      .foreach(x => print(s"${x} "))

    // using intersection()
    println("\nMy Lists With intersection (kesişim) :")
    myList3RDD
      .intersection(myList4RDD)
      .take(10)
      .foreach(x => print(s"${x} "))

    // using subtract()
    println("\nMy Lists With subtract (fark) :")
    myList3RDD
      .subtract(myList4RDD)
      .take(10)
      .foreach(x => print(s"${x} "))

    // using cartesian()
    println("\nMy Lists With cartesian :")
    myList3RDD
      .cartesian(myList4RDD)
      .take(10)
      .foreach(x => print(s"${x} "))

    // using collect()
    println("\nMy Lists With collect :")
    myList3RDD
      .collect()
      .foreach(x => print(s"${x} "))

    // using count()
    println("\nMy Lists With count :")
    print(myList3RDD.count())

    // using countByValue()
    println("\nMy Lists With countByValue :")
    print(myList3RDD.countByValue())

    // using take()
    println("\nMy Lists With Take :")
    myList3RDD
      .take(3)
      .foreach(x => print(s"${x} "))

    // using top()
    println("\nMy Lists With top :")
    myList3RDD
      .top(4)
      .foreach(x => print(s"${x} "))

    // using takeOrdered()
    println("\nMy Lists With takeOrdered :")
    myList3RDD
      .takeOrdered(20)
      .foreach(x => print(s"${x} "))

    // using takeSample()
    println("\nMy Lists With takeSample :")
    myList3RDD
      .takeSample(withReplacement = false, 5, 33)
      .foreach(x => print(s"${x} "))

    // using reduce()
    println("\nMy Lists With reduce :")
    print(myList3RDD.reduce((x, y) => x + y))
    //print(myList3RDD.reduce(_+_))

    // using fold()
    println("\nMy Lists With fold :")
    print(myList3RDD.fold(0)((x, y) => x + y))
    //print(myList3RDD.fold(0)(_+_))

    // using aggregate()
    println("\nMy Lists With aggregate :")
    val aggregatedRDD: (Int, Int) = myList3RDD.aggregate((0, 0))(
      (acc, value) => (acc._1 + value, acc._1 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    print(aggregatedRDD)
    sc.stop()


  }


}
