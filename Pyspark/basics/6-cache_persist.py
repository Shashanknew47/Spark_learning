from pyspark import SparkContext,StorageLevel

sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/poem.txt"


file_load_rdd = sc.textFile(filepath).flatMap(lambda x:x.split()).map(lambda x:x.lower())
file_load_tuple = file_load_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:(a+b)).sortBy(lambda x:x[1],False)


# persist the rdd
file_persist_count_rdd = file_load_tuple.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

count_rdd = file_persist_count_rdd.count()

print(count_rdd)

for i in file_persist_count_rdd.take(20):
    print(i)
