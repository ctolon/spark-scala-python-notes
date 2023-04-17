"""Top Level Module For Prepare Schema"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Build SparkSession
spark = SparkSession.builder \
    .appName('Spaceship.Comp') \
    .config("spark.executor.memory", "16g") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()
    
# Set Log Level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Data Paths
train_path = "data/1-Schemeless-Raw-Data/train.csv"
test_path = "data/1-Schemeless-Raw-Data/test.csv"

# Write To Disk
new_train_path = f"{os.getcwd()}/train_age_transform"
new_test_path = f"{os.getcwd()}/test_age_transform"


# Read Datas
trainDF_raw_1 = spark.read.format("csv") \
       .option("header", "true") \
       .option("inferSchema", "True") \
       .load(train_path) 

testDF_raw_1 = spark.read.format("csv") \
       .option("header", "true") \
       .option("inferSchema", "True") \
       .load(test_path)
              
# Preprocess for prepare data for schema
trainDF_raw_1 = trainDF_raw_1.withColumn("Age", F.col("Age").cast("integer"))
trainDF_raw_1 = trainDF_raw_1.withColumn("Age", F.floor(F.col("Age")))
testDF_raw_1 = testDF_raw_1.withColumn("Age", F.col("Age").cast("integer"))
testDF_raw_1 = trainDF_raw_1.withColumn("Age", F.floor(F.col("Age")))

trainDF_raw_1 \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header","true") \
    .csv(new_train_path)

testDF_raw_1 \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header","true") \
    .csv(new_test_path)
    
# Read Clean Datas For testing
trainDF_raw_2 = spark.read.format("csv") \
       .option("header", "true") \
       .option("inferSchema", "True") \
       .load(new_train_path) 

testDF_raw_2 = spark.read.format("csv") \
       .option("header", "true") \
       .option("inferSchema", "True") \
       .load(new_test_path)
       
testDF_raw_2.show()
trainDF_raw_2.show()