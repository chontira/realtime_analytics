import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import functools 
import math




if __name__=="__main__":
    sc = SparkContext(appName="Kafka Spark Demo")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc,1)
    ssc.checkpoint("checkpoint")
    
    initialDocID = sc.parallelize([(None,(0,'test'))])

    def updateID(new_val, last_val):
        lines = ''
        if len(new_val) > 0:
            ID = last_val[0]+1
            for item in new_val:
                lines+=item+' '
            lines = lines[:-1]
        else:
            ID = last_val[0]
        return (ID, lines)
    
    def updateWC(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    def countID(new_values, last_sum):
        if len(new_values) > 0:
            ID = 1 + (last_sum or 0)
        else:
            ID = last_sum
        return ID
    
    # def computeIDF(d):
    #     # global broadcastID
    #     return math.log10(broadcastID/d)
    brokerUrl= "tcp://gb.netpie.io:1883"
    topic = "/streams-plaintext-input"

    # Receive data as (None,text)
    msg = KafkaUtils.createDirectStream(ssc, topics=["streams-plaintext-input"],kafkaParams={"metadata.broker.list":"localhost:9092"})
    # msg = MQTTUtils.createStream(ssc, brokerUrl, topic)

    # map text in same windows to same id
    # (None,text1),(None,text2),... -> (ID,texts)
    line = msg.updateStateByKey(updateID, initialRDD=initialDocID).map(lambda x: x[1])
    # ID = msg.updateStateByKey(countID).map(lambda x: x[1])
    # ID.pprint()
    # broadcastID=sc.broadcast(ID)

    # Compute TF
    # (ID,texts) -> ((ID,token),1/(#word in doc)) -> ((ID,token),[1/(#word in doc) + 1/(#word in doc) + ...])
    map1=line.flatMap(lambda x: [((x[0],i),1/len(x[1].split())) for i in x[1].split()]).updateStateByKey(updateWC)
    # map1.pprint()

    # ((ID,token),TF) -> (token,(ID,TF))
    tf=map1.map(lambda x: (x[0][1],(x[0][0],x[1])))
    # tf.pprint()

    # Compute IDF
    # ((ID,token),TF) ->  (token,1)
    map2 = map1.map(lambda x: (x[0][1],1))
    # map2.pprint()

    # (token,1) -> (token,[1+1+1+...])
    reduce2 = map2.reduceByKey(lambda x,y:x+y)
    # reduce2.pprint()

    # (token,max ID)
    ID_token = map1.map(lambda x: (x[0][1],x[0][0])).reduceByKey(max)
    # ID_token.pprint()

    # (token,#Doc contain word) + (token,max ID) = (token,(#Doc contain word,max ID))
    reduce2_maxID = reduce2.join(ID_token)
    # reduce2_maxID.pprint()

    # (token,(#Doc contain word,max ID)) -> (token,IDF)
    idf=reduce2_maxID.map(lambda x: (x[0],math.log10(x[1][1]/x[1][0])))
    # idf.pprint()

    # Computing TD-IDF
    # (token,(ID,TF),IDF) -> (ID,(token,TF,IDF,TF-IDF)) 
    rdd=tf.join(idf).map(lambda x: (x[1][0][0],(x[0],x[1][0][1],x[1][1],x[1][0][1]*x[1][1])))

    # (ID,token,TF,IDF,TF-IDF)
    rdd=rdd.map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3]))
    rdd.pprint()

    
    ssc.start()
    ssc.awaitTermination()
