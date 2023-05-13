# As this is streaming app. So, first we need to start a sreaming to connect with it.
# use nc -lk 9998  => this command will create a stream on a socket(IP + port no.9998)


from pyspark import *
from pyspark.streaming import *

# you need at least 2 cores to run spark streaming application 
sc = SparkContext("local[2]","streaming_app")

sc.setLogLevel("ERROR")

# creating streaming context
ssc = StreamingContext(sc,5)   # second argument is duration of batch in seconds


# lines is a Dstream
lines = ssc.socketTextStream("localhost",9998)

words = lines.flatMap(lambda x:x.split(" "))

pairs = words.map(lambda x:(x,1))

wordCount = pairs.reduceByKey(lambda x,y:(x + y))

wordCount.pprint()
ssc.start()
ssc.awaitTermination()