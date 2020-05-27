import pyspark as spark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("3lab") \
        .master("local[*]") \
        .getOrCreate()

    # read data into a DataFrame
    dataset = spark.read.csv('DS_2019_public.csv', header=True, inferSchema=True)
    new_df = dataset.replace([1, 2, 3, 4, 5], [0, 0, 1, 0, 0], 'Climate_Region_Pub')

    # Feature Engineering
    df_assembler = VectorAssembler(inputCols=['DIVISION', 'REPORTABLE_DOMAIN', 'TOTALDOLCOL', 'KWHCOL', 'BTUELCOL'],
                                   outputCol="features")
    new_df = df_assembler.transform(new_df)
    # dataset.printSchema()

    climate_region_indexer = StringIndexer(inputCol="Climate_Region_Pub", outputCol="Climate_Region_Pub_Num").fit(
        new_df)
    new_df = climate_region_indexer.transform(new_df)

    climate_region_encoder = OneHotEncoder(inputCol="Climate_Region_Pub_Num", outputCol="Climate_Region_Pub_Vector")
    new_df = climate_region_encoder.transform(new_df)
    new_df.show()

    # Model
    model_df = new_df.select(['features', 'Climate_Region_Pub'])
    train_df, test_df = model_df.randomSplit([0.75, 0.25])

    # Build and train
    rf_classifier = RandomForestClassifier(labelCol='Climate_Region_Pub').fit(train_df)
    log_reg = LogisticRegression(labelCol='Climate_Region_Pub').fit(train_df)

    # Evaluation on Test Data
    rf_predictions = rf_classifier.transform(test_df)
    # rf_predictions.show()
    # rf_predictions.groupBy('prediction').count().show()

    logreg_results = log_reg.evaluate(test_df).predictions
    # results.printSchema()
    # results.select(['Climate_Region_Pub', 'prediction']).show(10, False)

    print('RF Feature Importances:')
    print(rf_classifier.featureImportances)
    print(new_df.schema["features"].metadata["ml_attr"]["attrs"], '\n')

    print('Random Forest')
    rf_accuracy = MulticlassClassificationEvaluator(labelCol='Climate_Region_Pub', metricName='accuracy').evaluate(
        rf_predictions)
    print('Random Forest Accuracy {0:.0%}'.format(rf_accuracy), '({})'.format(rf_accuracy))

    rf_precision = MulticlassClassificationEvaluator(labelCol= 'Climate_Region_Pub',
                                                     metricName='weightedPrecision').evaluate(rf_predictions)
    print('Random Forest Precision {0:.0%}'.format(rf_precision), '({})'.format(rf_precision))

    rf_recall = MulticlassClassificationEvaluator(labelCol='Climate_Region_Pub',metricName='weightedRecall').evaluate(
        rf_predictions)
    print('Random Forest Recall {0:.0%}'.format(rf_recall), '({})'.format(rf_recall), '\n')

    print('Logistic Regression')
    lr_accuracy = MulticlassClassificationEvaluator(labelCol='Climate_Region_Pub', metricName='accuracy').evaluate(
        logreg_results)
    print('Logistic Regression Accuracy {0:.0%}'.format(lr_accuracy), '({})'.format(lr_accuracy))

    lr_precision = MulticlassClassificationEvaluator(labelCol='Climate_Region_Pub',
                                                     metricName='weightedPrecision').evaluate(logreg_results)
    print('Logistic Regression Precision {0:.0%}'.format(lr_precision), '({})'.format(lr_precision))

    lr_recall = MulticlassClassificationEvaluator(labelCol='Climate_Region_Pub', metricName='weightedRecall').evaluate(
        logreg_results)
    print('Logistic Regression Recall {0:.0%}'.format(lr_recall), '({})'.format(lr_recall))