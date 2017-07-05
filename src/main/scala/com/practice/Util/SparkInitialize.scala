package com.practice.Util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/1/2017.
  */
object SparkInitialize {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)




  }
}
