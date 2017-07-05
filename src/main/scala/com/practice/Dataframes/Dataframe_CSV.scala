package com.practice.Dataframes

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vdokku on 7/1/2017.
  */
object Dataframe_CSV {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf().setAppName("<< Spark Loading of CSV >>").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // This is for loading the CSV files.

    // load the data into an RDD
    // For the relative path, I need to start form the src/main/resouces/csv files.....

    // So from 1.6 sqlContext comes with the read option & then the OPTIONS for reading the data.
    val df = sqlContext.read.format("com.databricks.spark.csv").options(Map("path" -> "src/main/resources/sfpd.csv", "header" -> "true")).load()
    df.show(12)
    println(df.count()) // Total count is 9999.


    /**
      * +----------+--------------+--------------------+---------+----------+-----+----------+--------------+--------------------+-----------------+----------------+--------------------+--------------+
      * |IncidntNum|      Category|            Descript|DayOfWeek|      Date| Time|PdDistrict|    Resolution|             Address|                X|               Y|            Location|          PdId|
      * +----------+--------------+--------------------+---------+----------+-----+----------+--------------+--------------------+-----------------+----------------+--------------------+--------------+
      * | 150467944| LARCENY/THEFT|GRAND THEFT FROM ...| Thursday|05/28/2015|23:59|TENDERLOIN|          NONE|TAYLOR ST / OFARR...|-122.411328369311|37.7859963050476|(37.7859963050476...|15046794406244|
      * | 150468629| VEHICLE THEFT|        STOLEN TRUCK| Thursday|05/28/2015|23:59|   MISSION|          NONE|100 Block of PORT...|-122.444276468858|37.7484965077695|(37.7484965077695...|15046862907025|
      */


    //Find the categories in the inputdata.
//    df.select("Category").distinct().collect().foreach(println)

    /*[MISSING PERSON]
    [RUNAWAY]
    [FRAUD]
    [WEAPON LAWS]
    [LIQUOR LAWS]
    [TRESPASS]
    [LOITERING]
    [BURGLARY]
    [SUSPICIOUS OCC]
    [LARCENY/THEFT]
    [OTHER OFFENSES]
    [ROBBERY]
    [PROSTITUTION]
    [KIDNAPPING]
    [DRIVING UNDER THE INFLUENCE]
    [SUICIDE]
    [GAMBLING]
    [VEHICLE THEFT]
    [EMBEZZLEMENT]
    [SEX OFFENSES, FORCIBLE]
    [SECONDARY CODES]
    [VANDALISM]
    [ARSON]
    [NON-CRIMINAL]
    [ASSAULT]
    [DRUNKENNESS]
    [STOLEN PROPERTY]
    [DRUG/NARCOTIC]
    [DISORDERLY CONDUCT]
    [FORGERY/COUNTERFEITING]
    [BRIBERY]
    [WARRANTS]
    [FAMILY OFFENSES]
    [SEX OFFENSES, NON FORCIBLE]*/


    // Same thing using the SparkSQL.

    df.registerTempTable("CategoryTable")

//    sqlContext.sql("select distinct Category from CategoryTable").show(55)

    /*

Both of them gave the same output.

+--------------------+
|            Category|
+--------------------+
|      MISSING PERSON|
|             RUNAWAY|
|               FRAUD|
|         WEAPON LAWS|
|         LIQUOR LAWS|
|            TRESPASS|
|           LOITERING|
|            BURGLARY|
|      SUSPICIOUS OCC|
|       LARCENY/THEFT|
|      OTHER OFFENSES|
|             ROBBERY|
|        PROSTITUTION|
|          KIDNAPPING|
|DRIVING UNDER THE...|
|             SUICIDE|
|            GAMBLING|
|       VEHICLE THEFT|
|        EMBEZZLEMENT|
|SEX OFFENSES, FOR...|
|     SECONDARY CODES|
|           VANDALISM|
|               ARSON|
|        NON-CRIMINAL|
|             ASSAULT|
|         DRUNKENNESS|
|     STOLEN PROPERTY|
|       DRUG/NARCOTIC|
|  DISORDERLY CONDUCT|
|FORGERY/COUNTERFE...|
|             BRIBERY|
|            WARRANTS|
|     FAMILY OFFENSES|
|SEX OFFENSES, NON...|
+--------------------+

     */

      // Before finding out the top 10 resolution, I would like to know how many resolution are there ... like the disctinct one.


//    sqlContext.sql("select distinct Resolution from CategoryTable").collect().foreach(println)

    /*

    [LOCATED]
[ARREST, CITED]
[CLEARED-CONTACT JUVENILE FOR MORE INFO]
[JUVENILE CITED]
[EXCEPTIONAL CLEARANCE]
[JUVENILE BOOKED]
[UNFOUNDED]
[PSYCHOPATHIC CASE]
[ARREST, BOOKED]
[NONE]

     */

    // top 10 Resolutions

    sqlContext.sql("select Resolution, count(Resolution) as resCount from CategoryTable group by Resolution order by resCount desc limit 10")
              .foreach(println)

    /*

    [NONE,7278]
[ARREST, BOOKED,2393]
[UNFOUNDED,109]
[ARREST, CITED,100]
[JUVENILE BOOKED,55]
[LOCATED,28]
[EXCEPTIONAL CLEARANCE,19]
[PSYCHOPATHIC CASE,14]
[CLEARED-CONTACT JUVENILE FOR MORE INFO,2]
[JUVENILE CITED,1]

     */

    // Similar thing for the above, I want to get the top 12 categories, in the input data.
    sqlContext.sql("select Category, count(Category) as categoryCount from CategoryTable group by Category order by categoryCount desc limit 22").collect().foreach(println)


    /*

    [LARCENY/THEFT,2812]
[OTHER OFFENSES,1314]
[NON-CRIMINAL,1144]
[ASSAULT,815]
[VEHICLE THEFT,640]
[VANDALISM,513]
[BURGLARY,444]
[WARRANTS,417]
[SUSPICIOUS OCC,355]
[ROBBERY,256]
[DRUG/NARCOTIC,244]
[MISSING PERSON,224]





     */


  }
}
