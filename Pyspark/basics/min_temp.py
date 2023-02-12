from pyspark import SparkContext

sc = SparkContext("local[*]","movie_star_count")
sc.setLogLevel("ERROR")

file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/tempdata.csv"

def base_rdd(x):
    base = x.split(",")
    station_id = base[0]
    temperature = int(base[3])
    return (station_id,temperature)

station_temperature_rdd = sc.textFile(file_path).map(base_rdd).sortBy(lambda x:(x[0],x[1]))
sorted_rdd_value = station_temperature_rdd.map(lambda x:(x[0],(x[1],1)))



rdd_row_number = sorted_rdd_value.reduceByKey(lambda a,b: a if a < b else b )

for i in rdd_row_number.take(5):
    print(i)