package com.practice.examples

import com.practice.Util.Utills
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object WindowBasedStreaming {

  //nc -lk 9999

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Window-Based-Streaming").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))

    //ssc.checkpoint("/user/data/checkpoints/")

    val lines = ssc.socketTextStream("localhost", 9999)

    val messages = lines.window(Seconds(30), Seconds(10))

    messages.foreachRDD(
      rdd => {
        if (!rdd.isEmpty()) {
          println("rdd count  " + rdd.count())
          val path = "file:///opt/home/data/" + Utills.getTime()
          rdd.coalesce(1, false).saveAsTextFile(path)
        } else {
          println("Data is not yet recevied from the producer....")
        }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}