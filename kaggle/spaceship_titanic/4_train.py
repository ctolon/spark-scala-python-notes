# Src
from src.ml_pipelines import MLPipelines
from src.config import ColumnConfig, DataConfig, SparkConfig, SchemaConfig

def train():

    # Build SparkSession
    spark = SparkConfig.SPARK_SESSION
        
    # Set Log Level
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Read Datas
    trainDF = spark.read.format("csv") \
        .option("header", "true") \
        .schema(SchemaConfig.DATA_TRANSFORMED_SCHEMA) \
        .load(DataConfig.PREPROCESSED_DATA_TRAIN) 

    featuresDF = spark.read.format("csv") \
        .option("header", "true") \
        .load(DataConfig.SCALED_FEATURE_DATA)

    # Select Target Column For Train
    target_col = trainDF.select(ColumnConfig.TARGET_COLUMN)

    # Merge Feature - Target Column DataFrames
    X = target_col.join(featuresDF)
                    
    # Machine Learning Pipeline
    ml_pipeline = MLPipelines()

    rf_model = ml_pipeline.rf_ml_pipeline(
        train_df=X,
        target_column=ColumnConfig.TARGET_COLUMN,
        )

    sc.stop()

    #print("First Schema of Train: ")
    #trainDF.printSchema()
    #trainDF.show()

    #trainDF_copy = trainDF.select("*")
    #trainDF_copy.show(5)

    # NOT DROP the Target Column and select in test data as feature, ITS SPARK!
    # cols_to_drop = ['Transported']
    # print(f"Target Column Will Dropped : {cols_to_drop}")
    # X = trainDF_copy.drop(*cols_to_drop)
    #y = trainDF_copy.select("Transported")
        
    # train-test split for ML Prod
    #trainDF_prod, testDF_prod = train_test_split_spark(X, y)

    #print("Latest Train Schema:")
    #trainDF_copy.printSchema()
    #trainDF_copy.show(10)

    # y.printSchema()