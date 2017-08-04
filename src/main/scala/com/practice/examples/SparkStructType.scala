package com.practice.examples

import com.practice.Util.Utills
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
object SparkStructType {
  def main(args: Array[String]) {
    println("SparkStructType.main()")
    val conf = new SparkConf().setAppName("Spark-StructType-Example").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val person = sc.textFile(Utills.DATA_PATH +"person.txt")

    val schema = StructType(Array(StructField("firstName", StringType, true), StructField("lastName", StringType, true), StructField("age", IntegerType, true)))
    val rowRDD = person.map(_.split(",")).map(p => org.apache.spark.sql.Row(p(0), p(1), p(2).toInt))
    val personDF = sqlContext.createDataFrame(rowRDD, schema)
    personDF.registerTempTable("person")
    sqlContext.sql("select * from person").foreach(println)

    //saving as parquet file
    val path = Utills.DATA_PATH +"person-" + Utills.getTime()
    personDF.coalesce(1).write.parquet(path)

    //saving DataFrame as Text file
    //personDF.rdd.coalesce(1, false).saveAsTextFile(path)
    
    //reading a parquet file 
    val parqfileDF = sqlContext.read.parquet(path)
    parqfileDF.filter("age > 25").show()
    val df = parqfileDF.groupBy("firstName", "lastName").agg(sum(parqfileDF.col("age")))
    df.show()

  }
}