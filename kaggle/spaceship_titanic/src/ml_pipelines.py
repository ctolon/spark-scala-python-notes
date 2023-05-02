
# PySpark
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import DataFrame

# System Packages
from typing import Dict, List

class MLPipelines(object):
    
    def rf_ml_pipeline(
        self, 
        train_df: DataFrame,
        target_column: str,
        ) -> Dict[str, CrossValidatorModel]:
        
        if train_df is None or not isinstance(train_df, DataFrame):
            raise TypeError(f"Train DataFrame must be type in Spark DataFrame!")
                
        print("Random Forest Gradient Boosting starting..")
        
        # Hyper Parameters
        grid_params = [100, 200, 300]
                    
        # Define Machine Learning Models
        rf = RandomForestClassifier(featuresCol="scaled_features", labelCol=target_column, seed=1)
                        
        # Define the Random Forest and Gradient Boosting parameter grids
        #rf_param_grid = ParamGridBuilder() \
            #.addGrid(rf.numTrees, grid_params) \
            #.build()
                                    
        # OPTIONAL: Define the CrossValidator for each algorithm
        """
        rf_cv = CrossValidator(estimator=machine_learning_pipeline,
                               estimatorParamMaps=rf_param_grid,
                               evaluator=bce,
                               numFolds=10)
        
        print(f"Cross Validator: {rf_cv}")
        """

        # Fit the models using the training data
        print("Train DF Schema Before Fitting:")
        train_df.printSchema()
        print("Fitting...")
               
        #rf_model = rf_cv.fit(train_df)
        rf_model = rf.fit(train_df)
        
        # Save Model
        rf_model.save("rf_model")
                
        output = {
            "Random_Forest_Model": rf_model,
            }

        return output


    def gbt_ml_pipeline(self, train_df: DataFrame):
        
        # Pre-Process Gradient Boosting
        gb_ss = StandardScaler(inputCol="features", outputCol="scaledFeatures")
        gb_va = VectorAssembler(inputCols='feature_cols', outputCol='features', handleInvalid='keep')

        # Define Machine Learning Models
        gb = GBTClassifier(featuresCol="scaledFeatures", labelCol="label", seed=1)
        
        gb_param_grid = ParamGridBuilder() \
            .addGrid(GBTClassifier.maxIter, [100, 200, 300]) \
            .build()
                    
        
        # Pipelines
        machine_learning_pipeline = Pipeline(stages=[gb_ss, gb_va, gb])
        

        gb_cv = CrossValidator(estimator=machine_learning_pipeline,
                               estimatorParamMaps=gb_param_grid,
                               evaluator=BinaryClassificationEvaluator(),
                               numFolds=10)

        # Fit the models using the training data
        gb_model = gb_cv \
            .fit(machine_learning_pipeline \
            .fit(train_df) \
            .transform(train_df))
            
        output = {
            ""
        }

        return {
            "Gradient_Boosting_Model": gb_model
            }
        
# machine_learning_pipeline = Pipeline(stages=[rf_si, rf_ohe ,rf_ss, rf_va, rf])
"""
# Define the StringIndexer for each categorical column
rf_si_hp = StringIndexer(inputCol='HomePlanet', outputCol='HomePlanet_index')
rf_si_cs = StringIndexer(inputCol='CryoSleep', outputCol='CryoSleep_index')
rf_si_d = StringIndexer(inputCol='Destination', outputCol='Destination_index')
rf_si_vip = StringIndexer(inputCol='VIP', outputCol='VIP_index')
rf_si_cd = StringIndexer(inputCol='CabinDeck', outputCol='CabinDeck_index')
rf_si_cs2 = StringIndexer(inputCol='CabinSide', outputCol='CabinSide_index')

# Define the OneHotEncoder for each categorical column
rf_ohe = OneHotEncoder(
    inputCols=[
        'HomePlanet_index',
        'CryoSleep_index',
        'Destination_index',
        'VIP_index',
        'CabinDeck_index',
        'CabinSide_index'
    ],
    outputCols=[
        'HomePlanet_vec',
        'CryoSleep_vec',
        'Destination_vec',
        'VIP_vec',
        'CabinDeck_vec',
        'CabinSide_vec'
    ]
)

# Define the VectorAssembler for the feature columns
rf_va = VectorAssembler(
    inputCols=[
        'HomePlanet_vec',
        'CryoSleep_vec',
        'Destination_vec',
        'Age',
        'VIP_vec',
        'RoomService',
        'FoodCourt',
        'ShoppingMall',
        'Spa',
        'VRDeck',
        'Transported',
        'CabinDeck_vec',
        'CabinSide_vec'
    ],
    outputCol='features'
)
"""

"""
rf_ss = StandardScaler(inputCol="features", outputCol="scaled_features")
rf_si = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in cat_columns]
rf_ohe = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_vec") for col in cat_columns]
rf_va = VectorAssembler(inputCols=[col+"_vec" for col in cat_columns] + num_columns + target_column_list, outputCol="features", handleInvalid='keep')

rf_ss = StandardScaler(inputCol="features", outputCol="scaled_features")
rf_si = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in cat_columns]
rf_si_fit = [si.fit(train_df) for si in rf_si]
rf_ohe = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_onehot") for col in cat_columns]
rf_va = VectorAssembler(inputCols=[col+"_onehot" for col in cat_columns] + num_columns + target_column_list, outputCol="features", handleInvalid='keep')


rf_si = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in cat_columns]
rf_ohe = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_vec") for col in cat_columns]
rf_ss = StandardScaler(inputCol="num_features", outputCol="scaled_numerical_features")
rf_va = VectorAssembler(inputCols=[col+"_vec" for col in cat_columns] + num_columns, outputCol="features", handleInvalid='keep')

# Define the StringIndexer and OneHotEncoderEstimator for each categorical column
rf_si = StringIndexer(inputCols=cat_columns, outputCols=rf_si_output, handleInvalid="keep")
rf_ohe = OneHotEncoder(inputCols=rf_si_output, outputCols=rf_ohe_output)

# Define the VectorAssembler for the numerical and categorical columns
rf_va = VectorAssembler(inputCols=rf_ohe_output + ["Transported"] + num_columns, outputCol=rf_va_output, handleInvalid='keep')
rf_ss = StandardScaler(inputCol=rf_va_output, outputCol=rf_ss_output)

# Define input lists for PreProcessors
rf_si_output = [element+"_index" for element in cat_columns]
rf_ohe_output = [element+"_onehot" for element in cat_columns]
rf_va_output = "features"
rf_ss_output = "scaled_features" 

print(rf_si_output)
print(rf_ohe_output)
print(rf_ohe_output + ["Transported"] + num_columns)

rf_model = rf_cv \
    .fit(machine_learning_pipeline \
    .fit(train_df) \
    .transform(train_df))
"""