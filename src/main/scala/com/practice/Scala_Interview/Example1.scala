package com.practice.Scala_Interview

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 8/6/2017.
  */
object Example1 {
  def main(args: Array[String]): Unit = {

    /**
      * Read the original text file,
      * seperated header file and data,
      * Input:
      * Key,V1,V2
      * A,1,2
      * B,2,3
      *
      * Output:
      * Key,Type,Value
      * A,V1,1
      * A,V2,2
      * B,V1,2
      * B,V2,3
      *
      */

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Calculate percentage >>").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val rdd = sc.textFile("/user/vishvaspatel34/original.txt") //Reading the Textfile
    val header = rdd.first() //header file
    val data = rdd.mapPartitionsWithIndex((index,itr)=>if(index==0) itr.drop(1) else itr) //Get only data, no header
    val l = header.split(",").toList //converted header into list
    val bl = sc.broadcast(l.slice(1,l.size)) //broadcasted the after key parts
    val preResultRDD = data.map(_.split(",")).flatMap(x=>(bl.value zip x.slice(1,x.size)).map(y=>(x(0),y))).map(x=>x._1+","+x._2.productIterator.mkString(","))
    val schemaRDD = sc.parallelize(List("Key,Type,Value"))
    val resultRDD = schemaRDD.union(preResultRDD)
    resultRDD.collect().foreach(println)

  }
}
