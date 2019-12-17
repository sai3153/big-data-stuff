package com.sparklearn.org

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._


object firstSpark {
  def main(args:Array[String]):Unit={
    val sc = SparkSession.builder().appName("myfirst").master("local").getOrCreate()
    val rdd1=Array("1ry","2hhjx","40xj","5tttt")

    val rdd2=rdd1.map(x=>{(x.length)})


    rdd2.take(10).foreach(println)
    sc.stop()


  }
}
