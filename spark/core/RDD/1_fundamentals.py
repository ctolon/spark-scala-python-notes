from pyspark.sql import SparkSession

pyspark = SparkSession.builder \
    .master("local[4]") \
    .appName("Create a RDD") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

sc = pyspark.sparkContext

rdd1 = sc.parallelize([("Ahmet", 25),("Berk", 18),("Mehmet", 28),("Batuhan", 20)])

rdd1.take(2)

sc.stop()

import pandas as pd
my_dict = {"Sayilar": [1,2,3,4,5], "Harfler": ["a","b","c","d","e"]}

pd_df = pd.DataFrame(my_dict)
pd_df.head()

rdd_from_pd_df = pyspark.createDataFrame(pd_df)
rdd_from_pd_df.show()

file_name = "OnlineRetail.csv"
rdd_text_file = sc.textFile(file_name)

rdd_text_file.take(10)

