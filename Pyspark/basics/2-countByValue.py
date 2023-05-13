from pyspark import SparkContext

sc = SparkContext("local[*]","movie_star_count")
sc.setLogLevel("ERROR")


file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/moviedata.data"

movie_rdd = sc.textFile(file_path).map(lambda x:(x.split("\t")[2]))

# countByValue will create a dictionary & countByValue is an action
star_count_rdd = movie_rdd.countByValue()

"""
Count the number of each star. Ex.
1,1,2,2,2,3,3,3,3

Result will be
(1,2)
(2,3)
(3,4)
"""
print(star_count_rdd)
