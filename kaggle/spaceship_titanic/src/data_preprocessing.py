"""Data Preprocessor Main Class."""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, BooleanType, FloatType
from pyspark.sql import DataFrame
from typing import List, Dict


class DataPreProcessPipeline(object):
    
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
            raise TypeError("[FATAL] Result not type of Spark DataFrame!!!")
        return result

    def pp_categorical_null_handler(self, categorical_columns: List[str], msg: str ='Missing') -> DataFrame:
    
        for i in categorical_columns:
            if self.df.schema[i].dataType == StringType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), msg).otherwise(F.col(i)))
        return self.df
    
    def pp_numerical_null_handler(self, num_columns: List[str]) -> DataFrame:
                
        # Replace null values in numerical columns with default value as -1 for double/float and 0 for integer
        for i in num_columns:
            if self.df.schema[i].dataType == DoubleType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), -1).otherwise(F.col(i)))
            elif self.df.schema[i].dataType == FloatType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), -1).otherwise(F.col(i)))
            elif self.df.schema[i].dataType == IntegerType():
                self.df = self.df.withColumn(i, F.when(F.col(i).isNull(), 0).otherwise(F.col(i)))                
        return self.df
        
    def pp_bool_encoding_integer(self) -> DataFrame:
        
        # Get boolean columns' names
        bool_columns = [col[0] for col in self.df.dtypes if col[1] == 'boolean']

        # Cast boolean to Integers
        for col in bool_columns:
            self.df = self.df.withColumn(col, F.col(col).cast(IntegerType()))
                
    def pp_single_bool_encoding_string(self, column_name: str) -> DataFrame:
        self.df = self.df.withColumn(column_name, F.when(self.df[column_name] == "true", 1).otherwise(0))
        return self.df
    
    def pp_bool_handler_string(self) -> DataFrame:
        
        bool_to_string_udf = F.udf(lambda x: str(x).lower(), StringType())
        bool_columns = [col for col in self.df.columns if self.df.schema[col].dataType == BooleanType()]
        self.df = self.df.select([bool_to_string_udf(F.col(c)).alias(c) if c in bool_columns else F.col(c) for c in self.df.columns])
        return self.df
                    
    def pp_median(self, column_name: str) -> DataFrame:
        
        # Fill NA Columns with median for Age Column
        median = self.df.approxQuantile(column_name, [0.5], 0.01)[0]
        self.df = self.df.withColumn(column_name, F.when(F.col(column_name).isNull(), int(median)).otherwise(F.col(column_name)))
        return self.df
    
    def pp_splitter(self, column_name: str, new_columns: Dict[str, int], splitter: str, msg: str) -> DataFrame:
         
        # Handle Missign Types                                       
        self.df = self.df.withColumn(column_name, F.when(F.col(column_name).isNull(), msg).otherwise(F.col(column_name)))
        
        # Produce New Features
        for new_column ,index in new_columns.items():
            self.df = self.df.withColumn(new_column, F.split(F.col(column_name), splitter).getItem(index))
        self.df = self.df.drop(column_name) # warn
        
        return self.df
    
    def pp_col_dropper(self, cols_to_drop: List[str]) -> DataFrame:
        
        self.df = self.df.drop(*cols_to_drop)
        return self.df
    
