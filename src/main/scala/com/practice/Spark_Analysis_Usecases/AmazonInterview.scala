package com.practice.Spark_Analysis_Usecases

import org.apache.spark.{SparkConf, SparkContext}

object AmazonInterview {
  System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Amazon-Interview").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sampleRDD = sc.parallelize(List(("blue black green blue black"), ("blue black green blue black")))
    sampleRDD.map(_.split(' ')).map(rec => rec(0))
    sampleRDD.collect().foreach(println)

    val s = List("hello world scala with spark hello spark scala world", "wow this is wow coz this is wow")
    sc.parallelize(s).map(r => r.split(" ")).map(r => f(r)).collect


  }

  def f(a: Array[String]): Array[(String, Int)] = {
    scala.util.Sorting.quickSort(a)

    def g(b: List[String]): List[List[String]] = {
      b match {
        case Nil => Nil
        case x :: Nil => List(List(x))
        case h :: tail => {
          val (f, l) = b.span(r => r == h)
          f :: g(l)
        }
      }
    }

    val c = g(a.toList)
    c.map(r => (r.head, r.length)).toArray
  }
}