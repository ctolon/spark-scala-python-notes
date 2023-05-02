# Src
from src.ml_preprocessing import MLPreProcessing

from src.config import ColumnConfig, DataConfig, SparkConfig, SchemaConfig

def ml_transform():

    # Build SparkSession
    spark = SparkConfig.SPARK_SESSION
    
    # Set Log Level
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Read Datas
    trainDF = spark.read.format("csv") \
        .option("header", "true") \
        .schema(SchemaConfig.DATA_FEATURIZED_SCHEMA) \
        .load(DataConfig.PREPROCESSED_DATA_TRAIN) 

    testDF = spark.read.format("csv") \
        .option("header", "true") \
        .schema(SchemaConfig.DATA_FEATURIZED_SCHEMA) \
        .load(DataConfig.PREPROCESSED_DATA_TEST)
                    
    # Machine Learning Pipeline
    ml_pipeline = MLPreProcessing()

    # Define Categorical and Numerical Columns.
    cat_cols_prod = ColumnConfig.CATEGORICAL_COLUMNS_FEATURIZED
    num_cols_prod = ColumnConfig.NUMERICAL_COLUMNS_FEATURIZED
    bool_cols_prod = ColumnConfig.BOOLEAN_COLUMNS
    target_column = ColumnConfig.TARGET_COLUMN
    
    train_df_rf_preprocessed = ml_pipeline.rf_ml_preprocessing(
        train_df=trainDF,
        target_column=target_column,
        num_columns=num_cols_prod,
        cat_columns=cat_cols_prod,
        bool_columns=bool_cols_prod
        )

    print("ML Pre-Processed Data:")
    print(train_df_rf_preprocessed.show())

    sc.stop()
    
if __name__ == "__main__":
    ml_transform()