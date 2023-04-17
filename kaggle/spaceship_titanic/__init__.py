# transform the data through each stage of the pipeline and print the schema
"""
for i, stage in enumerate(machine_learning_pipeline.getStages()):
    print(f"Pipeline stage {i+1}: {type(stage).__name__}")
    if isinstance(stage, StringIndexer):
        train_df.select(stage.getOutputCol()).printSchema()
    else:
        train_df = stage.transform(train_df)
        train_df.printSchema()
"""
            