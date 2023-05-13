# Join will be done on the key

# Files :
# ratings.dat : user_id,movie_id,star_rating, timestamp
# movies-dat.dat : movie_id, name, other details

"""Task :
    Find the movie name:
        - with average rating more than 4.5
        - At least  1000 people have rated that movie
"""

from pyspark import SparkContext

sc = SparkContext("local[*]")
sc.setLogLevel("ERROR")

ratings_file  = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/ratings.dat"
movies_file = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/movies-dat.dat"

def movieId_rating(line):
    line = line.split('::')
    movie_id = line[1]
    rating = int(line[2])
    return (movie_id,(rating,1))


def count_sum(a,b):
    return ((a[0]+b[0]),(a[1]+b[1]))


ratings_rdd = sc.textFile(ratings_file).map(movieId_rating).reduceByKey(count_sum).filter(lambda x:x[1][1]>1000)
avg_rating_rdd = ratings_rdd.map(lambda x:(x[0],(x[1][0]/x[1][1],x[1][1]))).filter(lambda x:x[1][0]>4)


def movieId_name(line):
    line = line.split('::')
    movie_id = line[0]
    movie_name = line[1]
    return (movie_id,movie_name)


movies_rdd = sc.textFile(movies_file).map(movieId_name)

# join will be done on the key
join_rdd = movies_rdd.join(avg_rating_rdd).map(lambda x:(int(x[0]),x[1])).sortByKey()

for i in join_rdd.take(15):
    print(i)
