from pyspark import SparkContext


filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/poem.txt"

unwanted_words = ['the','a','to','&','in','of','shall','who','he','from','is','are','that','and']

sc = SparkContext("local[*]","wordcount")
sc.setLogLevel("ERROR")
unwanted = sc.broadcast(unwanted_words)

file_load_rdd = sc.textFile(filepath).flatMap(lambda x:x.split()).map(lambda x:x.lower())
file_load_tuple = file_load_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:(a+b)).sortBy(lambda x:x[1],False)


final_words = file_load_tuple.filter(lambda x:x[0] not in unwanted.value)

x = final_words.take(20)

for i in x:
    print(i)
