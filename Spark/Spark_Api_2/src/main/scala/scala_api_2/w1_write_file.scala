package scala_api_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object w1_write_file {

  def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        sparkConf.set("spark.app.name","write_file")
        sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val file_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/orders.csv"

    val orderSchema = "order_id Int, order_date timestamp, order_customer_id Int, order_status String"

    val orderDf = spark.read.format("csv")
      .option("header",true)
      .schema(orderSchema)
      .option("path",file_path)
      .load

    /*
    you can repartition also this write, by adding this option .repartition(4).
     when you will write this dataFrame, then it will be number of files as per it's partition number
     So, if you will check on it's destination write. You can see that, it is saved in 4 partitions.

     advantage of repartition is, it will bring more parallelism. as number of executors is equal to number
     of partitions.
     */
    val orderDf_part = orderDf.repartition(4)

    /* In case you want to see how many partitions are there in a dataFrame
      Then you need to first convert it into Rdd. Then with getNumPartition you see how many
      partitions are there in a dataFrame.
      Ex. orderDf_part.rdd.getNumPartitions
     */


    // By default file will be saved parquet format.
    val dest_path = "/Users/shashankjain/Desktop/Practice/Spark_learning/Data_sets/write_spark/orders"
     orderDf_part.write.mode(SaveMode.Overwrite)
       .option("path",dest_path).save()

    orderDf.show()

  }

}
