from pyspark.sql import DataFrame

def train_test_split_spark(
    train: DataFrame,
    test: DataFrame,
    feature="Transported",
    seed=47,
    train_size= 0.75,
):
    
    # Drop the existing "features" column if it exists (for recall function)
    if "features" in train.columns:
        train = train.drop("features")
    #if "features" in test.columns:
        #test = test.drop("features")
        
    if "scaledFeatures" in train.columns:
        train = train.drop("scaled_features")
    #if "scaledFeatures" in test.columns:
        #test = test.drop("scaled_features")
                
    # Split the data into training and testing sets
    train, test = train.randomSplit([train_size, 1 - train_size], seed=seed)
    
    return train, test