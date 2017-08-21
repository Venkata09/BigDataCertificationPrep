package com.practice.Scala_Interview

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  *
  *
def g(b:List[String]):List[List[String]]= { b match {
case Nil => Nil
case x::Nil => List(List(x))
case h::tail => {
val (f,l) = b.span(r => r==h)
f :: g(l)
}
}}


  Forming a key with line number and each word in that line, and then do reduceByKey will give you the word count per line.



  * Created by vdokku on 8/6/2017.
  */
object Example2 {

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

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val s = List("hello world scala with spark hello spark scala world", "wow this is wow coz this is wow")
    sc.parallelize(s).map(r => r.split(" ")).map(r => f(r)).collect
  }
}
