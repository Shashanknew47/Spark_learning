package Streaming

// First go to terminal and use this command to start a streaming to catch data
// nc -lk 9995

// Remember that spark streaming application requires minimum 2 cores.
// For spark streaming we require a seperate streaming context also.

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds



object w_1_spark_streaming extends App{
        Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]","wordCount")

        val ssc = new StreamingContext(sc,Seconds(5))

        // As we are reading from socketText stream.
        // These lines are Dstreams here.
        val lines = ssc.socketTextStream("localhost",9995)

        val words = lines.flatMap(x => x.split(" "))

        val pairs = words.map(x => (x,1))

        val wordsCount = pairs.reduceByKey((x,y) => x + y)

        wordsCount.print()

        ssc.start()

        ssc.awaitTermination()

 
}
