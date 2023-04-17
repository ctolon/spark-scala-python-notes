# pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os

# Framework
from eda import ExploratoryDataAnalysis
from preprocessing import PreProcessPipeline
from train_test_split import train_test_split_spark
from ml_pipelines import MLPipelines
from schema import manual_schema

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

MAIN_DATA_DIR = "data/2-Schematic-Raw-Data"

# Data Paths
TRAIN_DATA_PATH = f"{MAIN_DATA_DIR}/train.csv"
TEST_DATA_PATH = f"{MAIN_DATA_DIR}/test.csv"

# Read Datas
trainDF = spark.read.format("csv") \
       .option("header", "true") \
        .schema(manual_schema) \
       .load(TRAIN_DATA_PATH) 

testDF = spark.read.format("csv") \
       .option("header", "true") \
        .schema(manual_schema) \
       .load(TEST_DATA_PATH)
       
target_column = "Transported"	
                  
# Instance of EDA and Cache CSV Files
eda = ExploratoryDataAnalysis(train_df=trainDF, test_df=testDF, strict=True)
eda.setCache()

print("First Schema of Train: ")
trainDF.printSchema()
trainDF.show()

"""
print("EDA REPORT....")
eda.print_train_raw_columns()
eda.print_test_raw_columns()
eda.print_pair_schemas()
eda.print_pair_counts()
eda.print_train_head()
eda.print_test_head()
eda.print_train_stats()
eda.print_test_stats()
"""


"""
# Get and Print Null Values
train_null_counts = eda.get_train_null_values
eda.print_train_null_values()

test_null_counts = eda.get_test_null_values
eda.print_test_null_values()

# Print By Type
eda.print_columns_by_type()


# Get and Print Unique Values
train_unique_counts = eda.get_train_distinct_count
train_unique_counts.show()

test_unique_counts = eda.get_test_distinct_count
test_unique_counts.show()
"""

trainDF_copy = trainDF.select("*")
trainDF_copy.show(5)

# Pre-Process on Data
pp = PreProcessPipeline(trainDF)

categorical_columns_pp = ['HomePlanet', 'CryoSleep', 'Destination', 'VIP', 'Name']
numerical_columns_pp = ['RoomService','FoodCourt','ShoppingMall','Spa','VRDeck']

pp.add_step(pp.pp_categorical_null_handler, categorical_columns=categorical_columns_pp, msg="Missing") \
    .add_step(pp.pp_numerical_null_handler, num_columns=numerical_columns_pp) \
    .add_step(pp.pp_bool_handler, bool_columns=categorical_columns_pp) \
    .add_step(pp.pp_single_bool_encoding, column_name=target_column) \
    .add_step(pp.pp_single_column_drop, column_name="PassengerId") \
    .add_step(pp.pp_hardcoded_preprocess)
    

trainDF_copy = pp.run()

print("Train Schema After Pre Process:")
trainDF_copy.printSchema()
trainDF_copy.show()

# NOT Drop the Target Column and select in test data as feature, ITS SPARK!
#cols_to_drop = ['Transported']
#print(f"Target Column Will Dropped : {cols_to_drop}")
#X = trainDF_copy.drop(*cols_to_drop)
y = trainDF_copy.select("Transported")
    
# train-test split for ML Prod
#trainDF_prod, testDF_prod = train_test_split_spark(X, y)

# Machine Learning Pipeline
ml_pipeline = MLPipelines()

print("Latest Train Schema:")
trainDF_copy.printSchema()
trainDF_copy.show(10)

# y.printSchema()

cat_cols_prod = ["HomePlanet", "CryoSleep", "Destination", "VIP", "CabinDeck", "CabinSide"]
num_cols_prod = ["Age", "RoomService", "FoodCourt", "ShoppingMall", "Spa", "VRDeck"] 
 
rf_model = ml_pipeline.rf_ml_pipeline(
    train_df=trainDF_copy,
    target_column=target_column,
    num_columns=num_cols_prod,
    cat_columns=cat_cols_prod
    )

sc.stop()