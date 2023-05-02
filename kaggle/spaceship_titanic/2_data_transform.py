import os

from src.config import ColumnConfig, DataConfig, SparkConfig, SchemaConfig
from src.data_preprocessing import DataPreProcessPipeline

def data_transform():

    # Build SparkSession
    spark = SparkConfig.SPARK_SESSION
        
    # Set Log Level
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Preprocessed Datas Write To Disk
    new_train_path = f"{os.getcwd()}/train_preprocessed"
    new_test_path = f"{os.getcwd()}/test_preprocessed"

    # Read Datas
    trainDF = spark.read.format("csv") \
        .option("header", "true") \
        .schema(SchemaConfig.DATA_RAW_SCHEMA) \
        .load(DataConfig.SCHEMATIC_DATA_TRAIN) 

    testDF = spark.read.format("csv") \
        .option("header", "true") \
        .schema(SchemaConfig.DATA_RAW_SCHEMA) \
        .load(DataConfig.SCHEMATIC_DATA_TEST)

    # Pre-Process on DataFrames
    pp_train = DataPreProcessPipeline(trainDF)
    pp_test = DataPreProcessPipeline(testDF)

    categorical_columns_pp = ColumnConfig.CATEGORICAL_COLUMNS
    numerical_columns_pp = ColumnConfig.NUMERICAL_COLUMNS
    
    cols_to_drop = ["PassengerId", "Name"]
    cabin_split_dict = {
        "CabinDeck": 0,
        "CabinSide": 2
    }
    
    # Remove Dropped and Preprocssed before null handlers        
    for column in cols_to_drop:
        categorical_columns_pp.remove(column)
    categorical_columns_pp.remove("Cabin")
    
    pp_train \
        .add_step(pp_train.pp_col_dropper, cols_to_drop=cols_to_drop) \
        .add_step(pp_train.pp_median, column_name="Age") \
        .add_step(pp_train.pp_splitter, column_name = "Cabin", new_columns=cabin_split_dict, splitter="/", msg="Missing/Missing/Missing") \
        .add_step(pp_train.pp_categorical_null_handler, categorical_columns=categorical_columns_pp, msg="Missing") \
        .add_step(pp_train.pp_numerical_null_handler, num_columns=numerical_columns_pp) \

    pp_test \
        .add_step(pp_test.pp_col_dropper, cols_to_drop=cols_to_drop) \
        .add_step(pp_test.pp_median, column_name="Age") \
        .add_step(pp_test.pp_splitter, column_name = "Cabin", new_columns=cabin_split_dict, splitter="/", msg="Missing/Missing/Missing") \
        .add_step(pp_test.pp_categorical_null_handler, categorical_columns=categorical_columns_pp, msg="Missing") \
        .add_step(pp_test.pp_numerical_null_handler, num_columns=numerical_columns_pp) \
    
    trainDF_copy = pp_train.run()
    testDF_copy = pp_test.run()

    # Write Schemas After Preproccessing
    print("Train Schema After Pre Process:")
    trainDF_copy.printSchema()
    trainDF_copy.show()

    print("Test Schema After Pre Process:")
    testDF_copy.printSchema()
    testDF_copy.show()

    # Write Schematic Datas to Disk
    trainDF_copy \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header","true") \
        .csv(new_train_path)

    testDF_copy \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header","true") \
        .csv(new_test_path)
        
    # Read Preprocessed Datas For testing
    trainDF_new = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "True") \
        .load(new_train_path) 

    testDF_new = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "True") \
        .load(new_test_path)
        
    print("Preprocessed Datas:")
    testDF_new.show()
    trainDF_new.show()
    
if __name__ == "__main__":
    data_transform()