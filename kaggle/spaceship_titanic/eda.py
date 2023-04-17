"""Top Level Module For Exploratory Data Analysis in Spark."""
# pyspark
from pyspark.sql import DataFrame
from typing import Optional, Tuple, List, Any
from utils import Singleton

class ExploratoryDataAnalysis(metaclass=Singleton):
    
    def __init__(self, train_df: Optional[DataFrame] = None, test_df: Optional[DataFrame] = None, strict: bool = False) -> None:        
        self.train_df = train_df
        self.test_df = test_df
        
        if train_df is not None and not isinstance(train_df, DataFrame):
            raise TypeError(f"Train DataFrame must be a Spark DataFrame! Provided type: {type(train_df)}")
        if test_df is not None and not isinstance(test_df, DataFrame):
            raise TypeError(f"Test DataFrame must be a Spark DataFrame! Provided type: {type(test_df)}")
        
        print(f"[INFO] EDA Strict Type Check: {strict}")
        if strict is True:
            if not isinstance(train_df, DataFrame):
                raise TypeError("In Strict Mode, train DataFrame must not be None!")
            if not isinstance(test_df, DataFrame):
                raise TypeError("In Strict Mode, test DataFrame must not be None!")

            
    def setCache(self):
        self.train_df.cache()
        self.test_df.cache()
        
    @property
    def get_pair_raw_columns(self) -> Tuple[List[str], List[str]]:
        """Get Columns from train-test Dataframes Directly."""
        return self.train_df.columns, self.test_df.columns
    
    @property
    def get_train_raw_columns(self) -> List[str]:
        """Get Columns of test Dataframe."""
        return self.train_df.columns
    
    @property
    def get_test_raw_columns(self) -> List[str]:
        """Get Columns of train DataFrame."""
        return self.test_df.columns
    
    def print_train_raw_columns(self) -> None:
        """Print Columns for train Spark DataFrame."""
        
        print("Train Columns:")
        print(self.train_df.columns)
        
    def print_test_raw_columns(self) -> None:
        """Print Columns for test Spark DataFrame."""
        
        print("Test Columns:")
        print(self.test_df.columns)

    @property
    def get_train_numerical_columns(self) -> List[str]:
        """Get Numerical Columns of train DataFrame."""
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        if self.train_df is None:
            raise TypeError(f"train DF is None!")
        train_num_columns = [column.name for column in self.train_df.schema if column.dataType in numTypes]
        return train_num_columns
    
    def print_train_numerical_columns(self) -> None:
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        if self.train_df is None:
            raise TypeError(f"train DF is None!")
        train_num_columns = [column.name for column in self.train_df.schema if column.dataType in numTypes]
        print(train_num_columns)
    
    @property
    def get_test_numerical_columns(self) -> List[str]:
        """Get Numerical Columns of test DataFrame."""
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        if self.test_df is None:
            raise TypeError(f"test DF is None!")
        test_num_columns = [column.name for column in self.test_df.schema if column.dataType in numTypes]
        return test_num_columns
        
    def print_test_numerical_columns(self) -> None:
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        if self.test_df is None:
            raise TypeError(f"test DF is None!")
        test_num_columns = [column.name for column in self.test_df.schema if column.dataType in numTypes]
        print(test_num_columns)
    
    @property
    def get_train_categorical_columns(self) -> List[str]:
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        train_categorical_columns = [column.name for column in self.train_df.schema if column.dataType not in numTypes]
        return train_categorical_columns
    
    def print_train_categorical_columns(self):
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        train_categorical_columns = [column.name for column in self.train_df.schema if column.dataType not in numTypes]
        return train_categorical_columns
    
    @property
    def get_test_categorical_columns(self) -> List[str]:
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        test_categorical_columns = [column.name for column in self.test_df.schema if column.dataType not in numTypes]
        return test_categorical_columns
    
    def print_test_categorical_columns(self) -> None:
        
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]
        test_categorical_columns = [column.name for column in self.test_df.schema if column.dataType not in numTypes]
        print(test_categorical_columns)
        
    def print_pair_schemas(self) -> None:
        
        self.test_df.printSchema()
        print("==========================")
        self.train_df.printSchema()
    
    def print_pair_counts(self) -> None:
        
        train_count = self.train_df.count()
        test_count = self.test_df.count()
        
        print("Train DF Count:")
        print(train_count)
        
        print("Test DF Count:")
        print(test_count)
    
    def print_train_head(self):
        
        print("TRAIN DF HEAD:")
        self.train_df.show(5)

    def print_test_head(self):
        
        print("TEST DF HEAD:")
        self.test_df.show(5)
        
    def print_train_stats(self):
        
        self.train_df.summary().toPandas()
        self.train_df.describe().toPandas()

    def print_test_stats(self):
        
        self.test_df.describe().toPandas()
        self.test_df.summary().toPandas()
    
    @property
    def get_train_null_values(self) -> Any:
        
        from pyspark.sql import functions as F
        train_null_counts = [(c, self.train_df.select(F.count(F.when(F.col(c).isNull(), c))).collect()[0][0]) for c in self.train_df.columns]
        return train_null_counts
        
    @property
    def get_test_null_values(self):
        
        from pyspark.sql import functions as F
        test_null_counts = [(column, self.test_df.filter(F.col(column).isNull()).count()) for column in self.test_df.columns]
        return test_null_counts
    
    def print_train_null_values(self):
        
        from pyspark.sql import functions as F
        train_null_counts = [(c, self.train_df.select(F.count(F.when(F.col(c).isNull(), c))).collect()[0][0]) for c in self.train_df.columns]
        print(train_null_counts)
    
    def print_test_null_values(self):
        
        from pyspark.sql import functions as F
        test_null_counts = [(column, self.test_df.filter(F.col(column).isNull()).count()) for column in self.test_df.columns]
        print(test_null_counts)
    
    def print_columns_by_type(self):
        
        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType, BooleanType, DecimalType, ByteType
        numTypes = [IntegerType(), FloatType(), DoubleType(), DecimalType(), ByteType()]

        print("===TRAIN COLUMNS===")
        train_string_columns = [column.name for column in self.train_df.schema if column.dataType == StringType()]
        train_bool_columns = [column.name for column in self.train_df.schema if column.dataType == BooleanType()]
        train_num_columns = [column.name for column in self.train_df.schema if column.dataType in numTypes]
        train_categorical_columns = train_string_columns + train_bool_columns

        print("Columns Per Dtype: ")
        print("-----------------------------")
        print("String Type Columns:")
        print(train_string_columns)
        print("Bool Type Columns:")
        print(train_bool_columns)
        print("Numerical Type Columns:")
        print(train_num_columns)

        print("\n=========================\n")

        print("Columns Categorical/Numerical:")
        print("-----------------------------")
        print("Categorical Columns:")
        print(train_categorical_columns)
        print("Numerical Columns:")
        print(train_num_columns)
    
    @property
    def get_train_distinct_count(self):
        
        from pyspark.sql import functions as F
        train_unique_counts = self.train_df.agg(*[F.countDistinct(c).alias(c) for c in self.train_df.columns])
        return train_unique_counts
    
    def print_train_distinct_count(self):
        
        from pyspark.sql import functions as F
        train_unique_counts = self.train_df.agg(*[F.countDistinct(c).alias(c) for c in self.train_df.columns])
        train_unique_counts.show()
        
    @property
    def get_test_distinct_count(self):
        
        from pyspark.sql import functions as F
        test_unique_counts = self.test_df.agg(*[F.countDistinct(c).alias(c) for c in self.test_df.columns])
        return test_unique_counts
        
    def print_test_distinct_count(self):
    
        from pyspark.sql import functions as F
        test_unique_counts = self.test_df.agg(*[F.countDistinct(c).alias(c) for c in self.test_df.columns])
        test_unique_counts.show()
        
    def get_values_group_by(self, df: DataFrame, column_name: Optional[str] = None):
        
        if not isinstance(column_name, str) or column_name is not None:
            raise TypeError("*******")
        
        if df == self.train_df:
            if column_name != "all_columns":
                return self.train_df.groupBy(column_name)
            return self.train_df.groupby("*")
        
        elif df == self.test_df:
            if column_name != "all_columns":
                return self.train_df.groupBy(column_name)
            return self.train_df.groupby("*")
        
        else:
            raise TypeError("!!!")
            
    @property
    def train_toPandasDF(self):
        return self.train_df.select("*").toPandas()
    
    @property
    def test_toPandasDF(self):
        return self.test_df.select("*").toPandas()