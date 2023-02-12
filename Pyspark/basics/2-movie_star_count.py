from pyspark import SparkContext

sc = SparkContext("local[*]","movie_star_count")
sc.setLogLevel("ERROR")


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/moviedata.data"

movie_rdd = sc.textFile(file_path).map(lambda x:(x.split("\t")[2]))

# countByValue will create a dictionary.
star_count_rdd = movie_rdd.countByValue()

print(star_count_rdd)
