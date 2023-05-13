from pyspark import SparkContext

sc = SparkContext(master="local[*]",appName="trial")
sc.setLogLevel("ERROR")


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/friendsdata.csv"


def age_number(x):
    x = x.split("::")
    age = x[2]
    number = int(x[3])
    return (age,(number,1))

rdd1 = sc.textFile(file_path).map(age_number).reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).map(lambda x:(x[0],x[1][0]/x[1][1])).take(20)

for i in rdd1:
    print(i)