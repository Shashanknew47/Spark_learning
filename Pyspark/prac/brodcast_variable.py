from pyspark import SparkContext


filepath = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/poem.txt"
boring_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/boringwords.txt"

sc = SparkContext("local[*]","count_numbers")
sc.setLogLevel("ERROR")

s = set()
with open(boring_path,'r') as f:
    for i in f:
        s.add(i.strip('\n'))


nameSet = sc.broadcast(s)

# Note brodcast variable is having value by which you can access main value of brodcast variable. check with print(nameSet.value)

base_rdd = sc.textFile(filepath).flatMap(lambda x:x.split(" "))\
            .filter(lambda x:not(x in nameSet.value))\
            .map(lambda x:(x,1))
reduced_rdd = base_rdd.reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],False)

for i in reduced_rdd.collect():
    print(i)