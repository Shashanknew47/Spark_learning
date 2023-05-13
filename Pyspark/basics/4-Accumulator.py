from pyspark import SparkContext

"""
accumulator is a shared variable which all the executors want to update.
Note: executors can only update. and not read them.

This is a normal counter in Python which can be updated by multiple executors.
which bring counter update with multiple parallelism

Here we want to count the blank lines in a file

"""

file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/samplefile.txt"

sc = SparkContext("local[*]","wordcount")
sc.setLogLevel("ERROR")


# this will give initially 0 value to accumulator
acc = sc.accumulator(0)

rdd1 =  sc.textFile(file_path)

rdd1.foreach(lambda x:acc.add(1) if x == '' else acc.add(0))

print(acc.value)
print(dir(rdd1))