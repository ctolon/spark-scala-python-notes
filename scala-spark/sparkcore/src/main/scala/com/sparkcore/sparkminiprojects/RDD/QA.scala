package com.sparkcore.sparkminiprojects.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/*Sorular

1-) SparkContext sınıfını kullanarak local modda çalışan 2 çekirdek, 2 Gb. driver, 3 Gb executor belleğine sahip, "Test" isimli ekrana "Merhaba Spark" yazan bir Spark uygulaması yazınız.

2-) 3,7,13,15,22,36,7,11,3,25 rakamlarından bir RDD oluşturunuz.

3-) "Spark'ı öğrenmek çok heyecan verici" cümlesinin tüm harflerini büyük harf yapınız.

4-) https://github.com/veribilimiokulu/udemy-apache-spark/blob/master/docs/Ubuntu_Spark_Kurulumu.txt adresindeki text dosyasını Spark ile okuyarak kaç satırdan oluştuğunu ekrana yazdırınız.

5-) https://github.com/veribilimiokulu/udemy-apache-spark/blob/master/docs/Ubuntu_Spark_Kurulumu.txt adresindeki text dosyasını Spark ile okuyarak kaç kelimeden oluştuğunu ekrana yazdırınız. (Kelimeler tekrarlanabilir)

6-) İkinci sorudaki rakam listesi ile 1,2,3,4,5,6,7,8,9,10 listesi arasındaki kesişim kümesini(ortak rakamları) Spark uygulaması ile ekrana yazdırınız.

7-) İkinci sorudaki rakamların tekil (rakamların tekrarlanmaması) halinden oluşan bir RDD yaratınız.

8-) İkinci sorudaki rakamların liste içinde kaçar kez tekrarlandıklarını (frekanslarını) bulan bir Spark uygulaması yazınız.*/


object QA {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("\nQ-1:")
    val sparkConf = new SparkConf()
      .setAppName("QA")
      .setMaster("local[2]")
      .set("spark.driver.memory", "2g")
      .setExecutorEnv("spark.executor.memory", "3g")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(Level.ERROR.toString)
    println("Merhaba Spark")

    println("\nQ-2:")
    val rddNums = sc.parallelize(List(3, 7, 13, 15, 22, 36, 7, 11, 3, 25))

    rddNums.foreach(println(_))

    println("\nQ-3: ")
    val textRDD = sc.makeRDD(List("Spark'ı öğrenmek çok heyecan verici"))
    textRDD.map(x => x.toUpperCase).foreach(println(_))

    println("\nQ-4: ")
    val ubuntuData = new File(getClass.getClassLoader.getResource("Ubuntu_Spark_Kurulumu.txt").getPath)
    val ubuntuDataPath = ubuntuData.toString
    val ubuntuRDD = sc.textFile(ubuntuDataPath)
    println(ubuntuRDD.count())

    println("\nQ-5: ")
    val wordNumberRDD = ubuntuRDD.flatMap(x => x.split(" ")).count()
    println(wordNumberRDD)

    println("\nQ-6: ")
    val rddNums2 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    rddNums.intersection(rddNums2).collect().foreach(println(_))

    println("\nQ-7: ")
    val primeNums = List(3, 7, 13, 15, 22, 36, 7, 11, 3, 25).distinct
    val rddPrimeNums = sc.parallelize(primeNums)
    rddPrimeNums.collect().foreach(println(_))

    println("\nQ-8: ")
    val numList = List(3, 7, 13, 15, 22, 36, 7, 11, 3, 25)
    val rddNumList = sc.parallelize(numList)
    rddNumList.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .collect().foreach(println(_))

    println("*************************************")
    sc.getConf.getAll.foreach(println(_))
    sc.stop()
  }

}
