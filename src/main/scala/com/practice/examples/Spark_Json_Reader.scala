package com.practice.examples

import com.practice.Util.Utills
import org.apache.spark.{SparkConf, SparkContext}
object Spark_Json_Reader {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark_Json_Reader").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val path = Utills.DATA_PATH +"sales.json"
    val salesDF = sqlContext.read.json(path)
    salesDF.registerTempTable("sales")
    val aggDF = sqlContext.sql("select sum(amountPaid) from sales")
    println(aggDF.collectAsList())
    
    val results = sqlContext.sql("SELECT customerId,itemName FROM sales ORDER BY itemName")
    // display dataframe in a tabular format
    results.show()
  }
}