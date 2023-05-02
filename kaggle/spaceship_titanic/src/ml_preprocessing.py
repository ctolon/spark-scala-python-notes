
# PySpark
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder, FeatureHasher
from pyspark.ml import Pipeline

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# System Packages
from typing import Dict, List

class MLPreProcessing(object):
    
    def rf_ml_preprocessing(
        self, 
        train_df: DataFrame,
        target_column: str,
        num_columns: List[str],
        cat_columns: List[str],
        bool_columns: List[str],
        ) -> DataFrame:
        
        if train_df is None or not isinstance(train_df, DataFrame):
            raise TypeError(f"Train DataFrame must be type in Spark DataFrame!")
                        
        print("Categorical Columns:")
        print(cat_columns)
        
        print("Numerical Columns:")
        print(num_columns)
        
        print("Boolean Columns:")
        print(bool_columns)
        
        print(f"Target Column: {target_column}")
        
        # Drop Target Column For Training
        cols_to_drop = [target_column]
        print(f"Target Column Will Dropped : {cols_to_drop}")
        X = train_df.drop(*cols_to_drop)
                            
        # Pre-Processors For Random Forest
        
        # Define the StringIndexer for each categorical column
        rf_si = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in cat_columns]
        
        # Define OneHotEncoder for the categorical columns
        rf_ohe = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_onehot", dropLast=False) for col in cat_columns]
        
        # Define FeatureHasher for the boolean columns
        rf_fh: List[FeatureHasher] = []
        for bool_col in bool_columns:
            fh = FeatureHasher(inputCols=[bool_col], outputCol=bool_col+"_hash")
            rf_fh.append(fh)
        
        # Define the VectorAssembler for the numerical, categorical columns and boolean columns
        rf_va = VectorAssembler(
            inputCols=[col+"_onehot" for col in cat_columns] + [col+"_hash" for col in bool_columns] + num_columns,
            outputCol="features",
            handleInvalid="keep")
        
        # Define StandardScaler for Features
        rf_ss = StandardScaler(inputCol="features", outputCol="scaled_features")
                                           
        # Pipelines
        ml_preprocess_pipeline = Pipeline(stages=rf_si + rf_ohe + rf_fh + [rf_va, rf_ss])
                
        # Apply ML PreProcess For Training
        print("Train DF Schema Before ML Preprocessing:")
        train_df.printSchema()
               
        print("ML Preprocessing Transforms on going.....")
        
        
        # DEBUG
        for stage in ml_preprocess_pipeline.getStages():
            print(f"Applying transform: {stage.uid}")
            if not isinstance(stage, FeatureHasher) and not isinstance(stage, VectorAssembler):
                X = stage.fit(X).transform(X)
            else:
                X = stage.transform(X)
            X.printSchema()
            X.show(20)
               
        rf_model_preprocess = ml_preprocess_pipeline.fit(X)
        preprocessed_train_df = rf_model_preprocess.transform(X)
        
        print("Train DF Schema After ML Preprocessing:")
        preprocessed_train_df.printSchema()
        
        print("Train DF After Preprocessing:")
        preprocessed_train_df.show(20)
        
        # Select Feature Columns
        features = preprocessed_train_df.select("features")
        scaled_features = preprocessed_train_df.select("scaled_features")
        
        print("Features:")
        features.show(20, truncate=False)
        scaled_features.show(20, truncate=False)
        
        # Convert Vector column to String type for Writing to Disk.
        vector_to_string_udf = F.udf(lambda x: str(x), StringType())
        features_col = features.withColumn("features", vector_to_string_udf(F.col("features")))
        scaled_features_col = scaled_features.withColumn("scaled_features", vector_to_string_udf(F.col("scaled_features")))   
        
        # Save features/scaled features to disk
        features_col.write.csv("features")
        scaled_features_col.write.csv("scaled_features")
        
        # Save pipeline to disk
        #rf_model_preprocess.write().overwrite().save("rf_ml_preprocess_pipeline")
        
        return preprocessed_train_df