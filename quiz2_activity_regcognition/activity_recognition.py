import findspark
import numpy as np
from pyspark.mllib.stat import Statistics
from pyspark.sql.session import SparkSession
from pyspark.sql import Row, Column
from pyspark.ml import Pipeline, PipelineModel
# from pyspark.sql.functions import current_timestamp
from datetime import datetime
from pyspark.sql.functions import from_utc_timestamp
from pyspark.ml.feature import VectorAssembler
import time

import math
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__=="__main__":
    sc = SparkContext(appName="Kafka Spark Demo")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,5)
    ssc.checkpoint("checkpoint")
    spark = SparkSession(sc)

    def getSparkSessionInstance(sparkConf):
        if ("sparkSessionSingletonInstance" not in globals()):
            globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .getOrCreate()
        return globals()["sparkSessionSingletonInstance"]
    
    def toCSVLine(data):
        return ','.join(str(d) for d in data)
    
    def get_prediction(data):
        try:
            if data.count() > 0:
                # Get the singleton instance of SparkSession
                # now = datetime.now()
                # print("Start Time =", now.strftime("%H:%M:%S"))
                spark = getSparkSessionInstance(data.context.getConf())
                data_num = data.map(lambda x: (float(x[1]), float(x[2]),float(x[3])))
                maxtime = data.flatMap(lambda x:[x[0]]).reduce(max)
                # mintime = data.flatMap(lambda x:[x[0]]).reduce(min)
                # print(maxtime, data_num.count())
                # lines = data.map(toCSVLine)
                
                # lines.saveAsTextFile('dataset/walking/walking'+str(datetime.now()))
                summary = Statistics.colStats(data_num)
                # list_mean = summary.mean().tolist()
                list_sd = summary.variance().tolist()

                InputRow = Row(
                #     x_mean = list_mean[0], 
                #     y_mean = list_mean[1],
                #     z_mean = list_mean[2],
                    x_sd = math.sqrt(list_sd[0]),
                    y_sd = math.sqrt(list_sd[1]),
                    z_sd = math.sqrt(list_sd[2]))
                InputDataFrame = spark.createDataFrame([InputRow])
                # InputDataFrame.show()

                label_dt_rf = {0.0:'walk', 1.0:'run', 2.0:'stand'}
                label_lr = {1.0:'run', 2.0:'walk', 3.0:'stand'}

                # decision tree
                # now = datetime.now()
                # print("Start DT =", now.strftime("%H:%M:%S"))
                # model_dt = PipelineModel.load('saved_model/DecisionTree')
                # predictions_dt = model_dt.transform(InputDataFrame)
                # result_dt = label_dt_rf[predictions_dt.select("prediction").collect()[0][0]]
                # print('decision tree prediction: ',result_dt)

                # # Random Forest
                # now = datetime.now()
                # print("Start RF =", now.strftime("%H:%M:%S"))
                # model_rf = PipelineModel.load('saved_model/RandomForest')
                # predictions_rf = model_rf.transform(InputDataFrame)
                # result_rf = label_dt_rf[predictions_rf.select("prediction").collect()[0][0]]
                # print('random forest prediction: ',result_rf)

                # # Logistic Regression
                # now = datetime.now()
                # print("Start LR =", now.strftime("%H:%M:%S"))
                model_lr = PipelineModel.load('saved_model/LogisticRegression')
                predictions_lr = model_lr.transform(InputDataFrame)
                result_lr = label_lr[predictions_lr.select("prediction").collect()[0][0]]
                print(maxtime+', '+result_lr)
                # now = datetime.now()
                # print("End time =", now.strftime("%H:%M:%S"))
                # print('===========================================')
        except Exception as e: 
          print(e)
    
    msg = KafkaUtils.createDirectStream(ssc, topics=["accelerometer-input"],kafkaParams={"metadata.broker.list":"localhost:9092"})
    # ['time','x','y','z']
    data = msg.map(lambda x: x[1].replace('b','').strip("'")).map(lambda x:  x.split(","))
    # data.pprint()
    data.foreachRDD(get_prediction)
    
    ssc.start()
    ssc.awaitTermination()
