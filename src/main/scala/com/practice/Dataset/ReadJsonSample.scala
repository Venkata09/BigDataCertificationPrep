package com.practice.Dataset

import org.apache.spark.{SparkConf, SparkContext}

// TODO: https://github.com/Re1tReddy/Spark/blob/master/Spark-2.1/input/schools.json


/**
  *
  *
  * https://github.com/Re1tReddy/Spark/blob/master/Spark-2.1/input/schools.json
  * Created by vdokku on 7/7/2017.
  */
object ReadJsonSample {

  case class University(name: String, numStudents: Long, yearFounded: Long)


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    /*val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val schools = sqlContext.read
      .json("C:\\Venkata_DO\\Spark_Practice_Excersies\\src\\main\\resources\\inputData\\schools.json")
      .as[University]
    schools.printSchema()*/

    /*
    +-----------+-----------+-----------+
|       name|numStudents|yearFounded|
+-----------+-----------+-----------+
|UC Berkeley|      37581|       1868|
|        MIT|      11318|       1860|
|     JNTU-A|      37581|       1950|
|      BITIT|      11318|       1999|
|        VIT|      37581|       1900|
|        VTU|      11318|       1900|
|        SRM|      37581|       1968|
|     SASTRA|      11318|       1990|
+-----------+-----------+-----------+

     */

    /*schools.show()


    val res = schools.map(s => s"${s.name} is  ${2017 - s.yearFounded} years old")
    res.foreach { x => println(x) }*/


  }
}
