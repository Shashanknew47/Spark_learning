from pyspark import *
from pyspark.streaming import *

sc = SparkContext("local[2]","statefull_app")

sc.setLogLevel("ERROR")

ssc = StreamingContext(sc,5)

lines = ssc.socketTextStream("localhost",9998)

ssc.checkpoint("/Users/shashankjain/Desktop/Practice/Spark_learning/Pyspark/Streaming/stream_state_data")

def updatefunc(newValues,previousState):
    if previousState is None:
        previousState = 0
    return sum(newValues,previousState)


words = lines.flatMap(lambda x:x.split(' '))
pairs = words.map(lambda x:(x,1))

wordCount = pairs.updateStateByKey(updatefunc)

wordCount.pprint()
ssc.start()
ssc.awaitTermination()
