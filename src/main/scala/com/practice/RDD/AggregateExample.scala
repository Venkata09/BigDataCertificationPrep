package com.practice.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by vdokku on 7/1/2017.
  */
object AggregateExample {
  def myfunc(index: Int, iter: Iterator[(String, Int)]): Iterator[String] = {
    iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("<<<< Aggregate by key test >>>>> ")
      .setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    val pairRDD = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)))

    //lets have a look at what is in the partitions
    pairRDD.mapPartitionsWithIndex(myfunc).collect.foreach(f => println(f))
    println("***********************************************")

    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(f => println(f))
    println("-----------------------------------------------")

    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(f => println(f))


  }
}
