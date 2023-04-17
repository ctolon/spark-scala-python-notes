from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType
from pyspark.sql import DataFrame
from typing import List


class PreProcessPipeline(object):
    
    def __init__(self, df: DataFrame):
        self.steps = []
        self.df = df

    def add_step(self, step_func, **step_kwargs):
        self.steps.append((step_func, step_kwargs))
        return self

    def run(self, **kwargs) -> DataFrame:
        for step_func, step_kwargs in self.steps:
            full_kwargs = step_kwargs.copy()
            full_kwargs.update(kwargs)
            result = step_func(**full_kwargs)
            
        if not isinstance(result, DataFrame):
            raise TypeError("!!!")
        return result

    def pp_categorical_null_handler(self, categorical_columns: List[str], msg: str ='Missing') -> DataFrame:
        
        # Replace null values in string columns with 'Missing'
        # categorical_columns = ['HomePlanet', 'CryoSleep', 'Destination', 'VIP', 'Name']
        for i in categorical_columns:
            if self.df.schema[i].dataType == StringType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), msg).otherwise(F.col(i)))
        return self.df
    
    def pp_bool_handler(self, bool_columns: List[str]) -> DataFrame:
        
        # Convert boolean values to string values for Post Processing
        bool_to_string_udf = F.udf(lambda x: str(x).lower(), StringType())
        bool_columns = [col for col in bool_columns if self.df.schema[col].dataType == BooleanType()]
        self.df = self.df.select([bool_to_string_udf(F.col(c)).alias(c) if c in bool_columns else F.col(c) for c in self.df.columns])
        return self.df
    
    def pp_single_bool_encoding(self, column_name: str) -> DataFrame:
        self.df = self.df.withColumn("Transported", F.when(self.df["Transported"] == "true", 1).otherwise(0))
        return self.df
        
    def pp_single_column_drop(self, column_name: str) -> DataFrame:
        self.df = self.df.drop(column_name)
        return self.df
        
    def pp_numerical_null_handler(self, num_columns: List[str]) -> DataFrame:
        
        # Replace null values in numerical columns with default value as -1 for double and 0 for integer
        # mon_columns = ['RoomService','FoodCourt','ShoppingMall','Spa','VRDeck']
        for i in num_columns:
            if self.df.schema[i].dataType == DoubleType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), -1).otherwise(F.col(i)))
            elif self.df.schema[i].dataType == IntegerType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), 0).otherwise(F.col(i)))
                
        return self.df

    def pp_hardcoded_preprocess(self) -> DataFrame:
        
        # Fill NA Columns with median for Age Column
        median_age = self.df.approxQuantile("Age", [0.5], 0.01)[0]
        self.df = self.df.withColumn('Age', F.when(F.col('Age').isNull(), int(median_age)).otherwise(F.col('Age')))
                                        
        # TODO fix it
        self.df = self.df.withColumn('Cabin', F.when(F.col('Cabin').isNull(), 'Missing/Missing/Missing').otherwise(F.col('Cabin')))
        self.df = self.df.withColumn('CabinDeck', F.split(F.col('Cabin'), '/').getItem(0))
        self.df = self.df.withColumn('CabinSide', F.split(F.col('Cabin'), '/').getItem(2))
        self.df = self.df.drop('Cabin', 'Name') # warn
        
        return self.df
    
