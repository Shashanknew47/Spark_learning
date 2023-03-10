package word_count

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object w2_word_count_final extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "word_count")
  val filePath = "/Users/shashankjain/Desktop/Practice/Linux/prac_fold/text_fold/FilesExercise/poem.txt"
  val input = sc.textFile(filePath)

  /*
  val words = input.flatMap(x => x.split(" "))
  val wordMap = words.map(x => (x,1))
  val finalCount = wordMap.reduceByKey((a,b)=>(a + b))
   */

  /* We can chain up these all steps in this way also.
  val final_count = input.flatMap(_.split(" ")).map((_,1)).reduceByKey((_ + _))

   */

  // We can write up the same like this also.

  val final_count = input.flatMap(_.split(" ")).
    map((_,1)).
    reduceByKey((_ + _))

  final_count.collect().foreach(println)

//  finalCount.collect.foreach(println)
  scala.io.StdIn.readLine()

}
