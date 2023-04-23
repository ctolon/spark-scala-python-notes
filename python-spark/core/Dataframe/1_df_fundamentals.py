from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
.appName("df_fundamentals") \
.config("spark.driver.memory", "2g") \
.config("spark.executor.memory", "4g") \
.master("local[*]") \
.getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

from pyspark.sql import Row
my_list = [1,2,3,4,5]
list_rdd = sc.parallelize(my_list) \
.map(lambda element: Row(element))

df_from_list = list_rdd.toDF(["numbers"])

df_from_list.show()

df_from_range = sc.parallelize(range(10,100,5)) \
.map(lambda element: (element,)) \
.toDF(["range"])

df_from_range.show(4)

from pyspark.sql.types import IntegerType
df_from_range2 = spark.createDataFrame(range(10,100,5), IntegerType())

df_from_file = spark.read \
.option("sep", ";") \
.option("header", "true") \
.option("inferSchema", "true") \
.csv(f"{os.getcwd()}/OnlineRetail.csv")

df_from_file.show()

df_from_file.printSchema()

df_from_file.count()

df_from_file.select(df_from_file.columns).show(10)

df_from_file.select("InvoiceNo","StockCode").show(10)

df_from_file.sort("InvoiceNo").show(10)

df_from_file.sort("InvoiceNo").explain()