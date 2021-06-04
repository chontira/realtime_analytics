# import libraries
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline


# instantiate spark environment
sc = SparkContext("local[*]", "Decision Tree Classifier")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

# load the dataset
df = spark.read.csv("dataset/mean_sd_rmout.csv", inferSchema=True, header=True).toDF("x_mean", "y_mean", "z_mean", "x_sd", "y_sd", "z_sd", "label", "activity", "position", "by")
df.show(3)

# transformer
vector_assembler = VectorAssembler(inputCols=["x_sd", "y_sd", "z_sd"],outputCol="features")
# df_temp = vector_assembler.transform(df)
# df_temp.show(3)

# drop the original data features column
# df = df_temp.drop("x_mean", "y_mean", "z_mean", "x_sd", "y_sd", "z_sd")
# df.show(3)

# estimator
l_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
df = l_indexer.fit(df).transform(df)
df.show(3)

# data splitting
(training,testing) = df.randomSplit([0.7,0.3])

print('#################################################################')
print('######################### decision tree #########################')
print('#################################################################')
# train our model using training data
dt = DecisionTreeClassifier(labelCol="labelIndex", featuresCol="features")
pipeline_dt = Pipeline(stages=[vector_assembler, dt])
pipelineFit_dt = pipeline_dt.fit(training)
pipelineFit_dt.write().overwrite().save('saved_model/DecisionTree')

# test our model and make predictions using testing data
predictions_dt = pipelineFit_dt.transform(testing)
predictions_dt.select("prediction", "labelIndex").show(5)

predictions_dt.select("labelIndex","activity").distinct().show()
# +----------+--------+
# |labelIndex|activity|
# +----------+--------+
# |       0.0|    walk|
# |       1.0|     run|
# |       2.0|   stand|
# +----------+--------+

evaluator_dt = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction",metricName="accuracy")
accuracy_dt = evaluator_dt.evaluate(predictions_dt)
print("Test Error = %g " % (1.0 - accuracy_dt))
print("Accuracy = %g " % accuracy_dt)

# Test Error = 0.0575133
# Accuracy = 0.942487

# Instantiate metrics object
metrics_dt = MulticlassMetrics(predictions_dt.select("prediction", "labelIndex").rdd)

# Overall statistics
print("Summary Stats")
print("Precision = %s" % metrics_dt.precision())
print("Recall = %s" % metrics_dt.recall())
print("F1 Score = %s" % metrics_dt.fMeasure())

# Precision = 0.9424867021276596                                               
# Recall = 0.9424867021276596
# F1 Score = 0.9424867021276596

label_list=[0.0, 1.0, 2.0]
for label in sorted(label_list):
    print("Class %s precision = %s" % (label, metrics_dt.precision(label)))
    print("Class %s recall = %s" % (label, metrics_dt.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics_dt.fMeasure(label, beta=1.0)))

# walk
# Class 0.0 precision = 0.8931799506984388
# Class 0.0 recall = 0.9619469026548673
# Class 0.0 F1 Measure = 0.9262888794205368
# run
# Class 1.0 precision = 0.9733096085409253
# Class 1.0 recall = 0.9063794531897266
# Class 1.0 F1 Measure = 0.9386529386529386
# stand
# Class 2.0 precision = 0.9805097451274363
# Class 2.0 recall = 0.9746646795827124
# Class 2.0 F1 Measure = 0.9775784753363228

print(metrics_dt.confusionMatrix().toArray())
# [[1087.   30.   13.]
#  [ 113. 1094.    0.]
#  [  17.    0.  654.]]

print('#################################################################')
print('######################### random forest #########################')
print('#################################################################')
# train our model using training data
rf = RandomForestClassifier(labelCol="labelIndex",featuresCol="features", numTrees=10)
pipeline_rf = Pipeline(stages=[vector_assembler, rf])
pipelineFit_rf = pipeline_rf.fit(training)
pipelineFit_rf.write().overwrite().save('saved_model/RandomForest')

# test our model and make predictions using testing data
predictions_rf = pipelineFit_rf.transform(testing)
predictions_rf.select("prediction", "labelIndex").show(5)
# +----------+----------+                                                         
# |prediction|labelIndex|
# +----------+----------+
# |       0.0|       2.0|
# |       0.0|       2.0|
# |       0.0|       2.0|
# |       0.0|       2.0|
# |       2.0|       2.0|
# +----------+----------+

predictions_rf.select("labelIndex","activity").distinct().show()
# +----------+--------+
# |labelIndex|activity|
# +----------+--------+
# |       0.0|    walk|
# |       1.0|     run|
# |       2.0|   stand|
# +----------+--------+

# evaluate the performance of the classifier
evaluator_rf = MulticlassClassificationEvaluator(labelCol="labelIndex",predictionCol="prediction", metricName="accuracy")
accuracy_rf = evaluator_rf.evaluate(predictions_rf)
print("Test Error = %g" % (1.0 - accuracy_rf))
print("Accuracy = %g " % accuracy_rf)

# Test Error = 0.0555186
# Accuracy = 0.944481

# Instantiate metrics object
metrics_rf = MulticlassMetrics(predictions_rf.select("prediction", "labelIndex").rdd)

# Overall statistics
print("Summary Stats")
print("Precision = %s" % metrics_rf.precision())
print("Recall = %s" % metrics_rf.recall())
print("F1 Score = %s" % metrics_rf.fMeasure())

# Precision = 0.9444813829787234                                                 
# Recall = 0.9444813829787234
# F1 Score = 0.9444813829787234

label_list=[0.0, 1.0, 2.0]
for label in sorted(label_list):
    print("Class %s precision = %s" % (label, metrics_rf.precision(label)))
    print("Class %s recall = %s" % (label, metrics_rf.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics_rf.fMeasure(label, beta=1.0)))

# walk
# Class 0.0 precision = 0.8989229494614748
# Class 0.0 recall = 0.9601769911504425
# Class 0.0 F1 Measure = 0.928540864356012
# run
# Class 1.0 precision = 0.9717813051146384
# Class 1.0 recall = 0.9130074565037283
# Class 1.0 F1 Measure = 0.9414780008543358
# stand
# Class 2.0 precision = 0.9805097451274363
# Class 2.0 recall = 0.9746646795827124
# Class 2.0 F1 Measure = 0.9775784753363228

print(metrics_rf.confusionMatrix().toArray())
# [[1085.   32.   13.]
#  [ 105. 1102.    0.]
#  [  17.    0.  654.]]

print('#######################################################################')
print ('######################### logistic regression #########################')
print('#######################################################################')
# Train a Logistic Regression model.
lr = LogisticRegression(featuresCol= 'features', labelCol= 'label') 
pipeline_lr = Pipeline(stages= [vector_assembler, lr])
pipelineFit_lr = pipeline_lr.fit(training)
pipelineFit_lr.write().overwrite().save('saved_model/LogisticRegression')

# test our model and make predictions using testing data
predictions_lr = pipelineFit_lr.transform(testing)
predictions_lr.select("prediction", "label").show(5)
# +----------+-----+                                                              
# |prediction|label|
# +----------+-----+
# |       2.0|    1|
# |       2.0|    1|
# |       2.0|    1|
# |       2.0|    1|
# |       1.0|    1|
# +----------+-----+

predictions_rf.select("label","activity").distinct().show()
# +-----+--------+                                                                
# |label|activity|
# +-----+--------+
# |    1|     run|
# |    2|    walk|
# |    3|   stand|
# +-----+--------+

# evaluate the performance of the classifier
evaluator_lr = MulticlassClassificationEvaluator(labelCol="label",predictionCol="prediction", metricName="accuracy")
accuracy_lr = evaluator_lr.evaluate(predictions_lr)
print("Test Error = %g" % (1.0 - accuracy_lr))
print("Accuracy = %g " % accuracy_lr)

# Test Error = 0.046875
# Accuracy = 0.953125

predictions_lr = predictions_lr.withColumn("label", predictions_lr["label"].cast("double"))
predictions_lr.select("prediction", "label").show(5)
# +----------+-----+                                                              
# |prediction|label|
# +----------+-----+
# |       2.0|  1.0|
# |       2.0|  1.0|
# |       2.0|  1.0|
# |       2.0|  1.0|
# |       1.0|  1.0|
# +----------+-----+

# Instantiate metrics object
metrics_lr = MulticlassMetrics(predictions_lr.select("prediction", "label").rdd)

# Overall statistics
print("Summary Stats")
print("Precision = %s" % metrics_lr.precision())
print("Recall = %s" % metrics_lr.recall())
print("F1 Score = %s" % metrics_lr.fMeasure())

# Precision = 0.953125      
# Recall = 0.953125
# F1 Score = 0.953125

label_list=[1.0, 2.0, 3.0]
for label in sorted(label_list):
    print("Class %s precision = %s" % (label, metrics_lr.precision(label)))
    print("Class %s recall = %s" % (label, metrics_lr.recall(label)))
    print("Class %s F1 Measure = %s" % (label, metrics_lr.fMeasure(label, beta=1.0)))

# run
# Class 1.0 precision = 0.957983193277311
# Class 1.0 recall = 0.9444904722452361
# Class 1.0 F1 Measure = 0.951188986232791
# walk
# Class 2.0 precision = 0.9349164467897977
# Class 2.0 recall = 0.9407079646017699
# Class 2.0 F1 Measure = 0.937803264225849
# stand
# Class 3.0 precision = 0.9750367107195301
# Class 3.0 recall = 0.9895678092399404
# Class 3.0 F1 Measure = 0.9822485207100592

print(metrics_lr.confusionMatrix().toArray())
# [[1140.   67.    0.]
#  [  50. 1063.   17.]
#  [   0.    7.  664.]]