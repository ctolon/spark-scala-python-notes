from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, FloatType

manual_schema = StructType(
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