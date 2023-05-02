"""Top Level Module For Prepare Schema"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import DataConfig, SparkConfig


def prepare_schema():

    # Build SparkSession
    spark = SparkConfig.SPARK_SESSION

    new_train_path = "schematic_train_data"
    new_test_path = "schematic_test_data"

    # Set Log Level
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Read Datas
    trainDF = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "True") \
            .load(DataConfig.SCHEMELESS_DATA_TRAIN) 

    testDF = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "True") \
            .load(DataConfig.SCHEMELESS_DATA_TEST)
                    
    # Preprocess for prepare data for schemas
    trainDF = trainDF.withColumn("Age", F.col("Age").cast("integer"))
    trainDF = trainDF.withColumn("Age", F.floor(F.col("Age")))
    testDF = testDF.withColumn("Age", F.col("Age").cast("integer"))
    testDF = trainDF.withColumn("Age", F.floor(F.col("Age")))

    # Write Schematic Datas to Disk
    trainDF \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header","true") \
    .csv(new_train_path)

    testDF \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header","true") \
    .csv(new_test_path)

    # Read Schematic Datas For testing
    trainDF_new = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "True") \
            .load(new_train_path) 

    testDF_new = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "True") \
            .load(new_test_path)
            
    print("Schematic Datas:")

    testDF_new.show()
    trainDF_new.show()
       
if __name__ == "__main__":
    prepare_schema()