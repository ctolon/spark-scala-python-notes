package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.File


case class JobSalary(job: String, salary: Double)

object AverageSalaryByJobs {
  def main(args: Array[String]): Unit = {

    // Set Logger Level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Build Spark Session
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("AverageSalary")
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

    // Read RDD
    val retailRDD = sc.textFile(dataPath)
      .filter(!_.contains("sirano"))

    retailRDD.take(5).foreach(println)

    val salaryPerJob = retailRDD.map(x => {
      val fields = x.split(",")
      val job = fields(3)
      val salary = fields(5).toDouble
      JobSalary(job, salary)
    })

    println("====================")
    salaryPerJob.map(x => (x.job, x.salary))
      .foreach(println)

    println("====================")
    salaryPerJob.map(x => (x.job, (x.salary, 1)))
      .foreach(println)

    println("====================")
    salaryPerJob.map(x => (x.job, (x.salary, 1)))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => (x._1 / x._2))
      .collect()
      .foreach(println)
  }

}
