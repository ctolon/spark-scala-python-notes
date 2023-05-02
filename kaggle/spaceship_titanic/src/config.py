from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, FloatType

class SparkConfig:
    
    SPARK_SESSION = SparkSession.builder \
        .appName('Spaceship.Comp') \
        .config("spark.executor.memory", "16g") \
        .master("local[*]") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
        
class DataConfig:
    
    SCHEMELESS_DATA_TRAIN = f"data/1-Schemeless-Raw-Data/train.csv"
    SCHEMELESS_DATA_TEST = f"data/1-Schemeless-Raw-Data/test.csv"
    
    SCHEMATIC_DATA_TRAIN = f"data/2-Schematic-Raw-Data/train.csv"
    SCHEMATIC_DATA_TEST = f"data/2-Schematic-Raw-Data/test.csv"
        
    PREPROCESSED_DATA_TRAIN = f"data/3-Preprocessed-Data/train.csv"
    PREPROCESSED_DATA_TEST = f"data/3-Preprocessed-Data/test.csv"
    
    FEATURE_DATA = f"data/4-Feature-Store/features.csv"
    SCALED_FEATURE_DATA = "data/4-Feature-Store/scaled-features.csv"
    
class SchemaConfig:
    
    DATA_RAW_SCHEMA = StructType(
    [
        StructField("PassengerId", StringType(), True),
        StructField("HomePlanet", StringType(), True),
        StructField("CryoSleep", BooleanType(), True),
        StructField("Cabin", StringType(), True),
        StructField("Destination", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("VIP", BooleanType(), True),
        StructField("RoomService", FloatType(), True),
        StructField("FoodCourt", FloatType(), True),
        StructField("ShoppingMall", FloatType(), True),
        StructField("Spa", FloatType(), True),
        StructField("VRDeck", FloatType(), True),
        StructField("Name", StringType(), True),
        StructField("Transported", BooleanType(), True),
    ]
    )
    
    DATA_FEATURIZED_SCHEMA = StructType(
    [
        StructField("HomePlanet", StringType(), True),
        StructField("CryoSleep", BooleanType(), True),
        StructField("Destination", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("VIP", BooleanType(), True),
        StructField("RoomService", FloatType(), True),
        StructField("FoodCourt", FloatType(), True),
        StructField("ShoppingMall", FloatType(), True),
        StructField("Spa", FloatType(), True),
        StructField("VRDeck", FloatType(), True),
        StructField("Transported", BooleanType(), True),
        StructField("CabinDeck", StringType(), True),
        StructField("CabinSide", StringType(), True),
    ]
    )
    

class ColumnConfig:
      
    CATEGORICAL_COLUMNS = ['PassengerId','HomePlanet','Cabin','Destination','Name']
    NUMERICAL_COLUMNS = ['Age','RoomService','FoodCourt','ShoppingMall','Spa','VRDeck']
    BOOLEAN_COLUMNS = ["CryoSleep", "VIP"]
    
    CATEGORICAL_COLUMNS_FEATURIZED = ['HomePlanet','Destination','CabinDeck','CabinSide']
    NUMERICAL_COLUMNS_FEATURIZED = ['Age','RoomService','FoodCourt','ShoppingMall','Spa','VRDeck']
    
    TARGET_COLUMN = "Transported"