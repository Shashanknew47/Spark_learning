from pyspark import SparkContext

sc = SparkContext("local[*]","movie_star_count")
sc.setLogLevel("ERROR")

file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/friendsdata.csv"

def base_data(connection_data):
    list_connections = connection_data.split("::")
    return (list_connections[2],(float(list_connections[3]),1))


age_collection = sc.textFile(file_path).map(base_data).reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1]))
# avg_age_collection = age_collection.map(lambda x:(x[0],x[1][0]/x[1][1]))

#! As now we just need to change the values. So, we can directly change values with mapValues
avg_age_collection = age_collection.mapValues(lambda x:x[0]/x[1])

for i in avg_age_collection.collect():
    print(i)
