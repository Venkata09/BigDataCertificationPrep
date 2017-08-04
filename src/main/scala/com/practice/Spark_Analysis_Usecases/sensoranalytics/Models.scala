package com.practice.Spark_Analysis_Usecases.sensoranalytics

import org.joda.time.DateTime


case class SensorRecord(dateTime: DateTime,
                      country:String,
                      state:String,
                      city:String,
                      sensorStatus:String)

case class CountryWiseStats(date: DateTime,country:String, count: BigInt)

case class StateWiseStats(date: DateTime,country:String,state:String, count: BigInt)

case class CityWiseStats(date: DateTime,city:String,sensorStatus:String, count: BigInt)