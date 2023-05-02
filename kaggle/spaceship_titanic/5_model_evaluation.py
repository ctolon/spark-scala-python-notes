
# PySpark
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml import PipelineModel

# Src
from src.schema import data_transformed_schema

# Build SparkSession
spark = SparkSession.builder \
    .appName('Spaceship.Comp') \
    .config("spark.executor.memory", "16g") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()
    
# Set Log Level
sc = spark.sparkContext
sc.setLogLevel("ERROR")
    
target_column = "Transported"	
MAIN_DATA_DIR = "data/3-Preprocessed-Data"

# Data Paths
TEST_DATA_PATH = f"{MAIN_DATA_DIR}/test.csv"

# Load the saved model
rf_model = PipelineModel.load("rf_model")

# Load the test data into a DataFrame
test_df = testDF = spark.read.format("csv") \
       .option("header", "true") \
       .schema(data_transformed_schema) \
       .load(TEST_DATA_PATH)
       

print("Test DF Head: ")
test_df.show(5)

# Use the loaded model to make predictions on the test data
predictions = rf_model.transform(test_df)

print("Columns for testDF:")
print(predictions.columns)

print("Predictions: ")
print(predictions.select("prediction").show())

print("rawPrediction: ")
print(predictions.select("rawPrediction").show())


# Evaluators

# Define the evaluator for AUC
evaluator_auc = BinaryClassificationEvaluator(labelCol=target_column, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
# Define the evaluator for accuracy
evaluator_accuracy = MulticlassClassificationEvaluator(labelCol=target_column, predictionCol="prediction", metricName="accuracy")
# Define the evaluator for precision
evaluator_precision = MulticlassClassificationEvaluator(labelCol=target_column, predictionCol="prediction", metricName="weightedPrecision")
# Define the evaluator for recall
evaluator_recall = MulticlassClassificationEvaluator(labelCol=target_column, predictionCol="prediction", metricName="weightedRecall")
# Define the evaluator for F1-score
evaluator_f1 = MulticlassClassificationEvaluator(labelCol=target_column, predictionCol="prediction", metricName="f1")

auc = evaluator_auc.evaluate(predictions)
accuracy = evaluator_accuracy.evaluate(predictions)
precision = evaluator_precision.evaluate(predictions)
recall = evaluator_recall.evaluate(predictions)
f1_score = evaluator_f1.evaluate(predictions)

# Print evaluation metrics
print("Metrics")
print("=============")
print(f"AUC: {auc}")
print(f"Accuracy: {accuracy}")
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print(f"F1-Score: {f1_score}")

sc.stop()