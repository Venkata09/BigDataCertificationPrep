package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/6/2017.
  */
object EmployeeData_JSON_Text {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // This is for loading the CSV files.

    // load the data into an RDD
    // For the relative path, I need to start form the src/main/resouces/csv files.....

    // So from 1.6 sqlContext comes with the read option & then the OPTIONS for reading the data.
    val employeeDF = sqlContext.read.json("src/main/resources/inputData/employee.json").toDF("Name", "age", "lastName")
    employeeDF.show(12)

    // Display only the NAME in data frame.
    employeeDF.select(employeeDF("Name")).show()

    // Display the employee by adding to more
    import sqlContext.implicits._
    // Even for performing the FILTER operation it';

    /*

    +----+---+-----------+
|Name|age|   lastName|
+----+---+-----------+
|John| 25|     Legend|
|John| 95|Ambikapathy|
+----+---+-----------+


     */
//    employeeDF.filter($"age" >= 25).filter(employeeDF("Name") === "John").show()

    /*
    +----------+---------+
|      Name|(age + 5)|
+----------+---------+
|Raghupathy|      100|
|      John|      100|
+----------+---------+
     */
    employeeDF.filter($"age" >= 95).select(employeeDF("Name"), employeeDF("age") + 5).show()

    // find the age between 20 to 40.


    employeeDF.registerTempTable("EmployeeTable")

    /*

    +----------+---+-----------+
|      Name|age|   lastName|
+----------+---+-----------+
|      John| 25|     Legend|
|      Mike| 45|   Anderson|
|    Robert| 55|    Timblin|
|   Delbert| 55|     Ramsey|
|Raghupathy| 25|     Ranjan|
|      Mike| 45|Velayoudhan|
|    Robert| 55|  Velmukham|
+----------+---+-----------+

     */

    sqlContext.sql("select * from EmployeeTable where age between 20 and 70").show()




  }
}
