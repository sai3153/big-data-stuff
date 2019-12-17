package com.sparklearn.org

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object dataFormats {
     def main(args:Array[String]):Unit={

       val spark=SparkSession.builder().appName("checkformat").master("local[*]").config("spark.sql.warehouse.dir","file:///C:/tmp").getOrCreate()
       val df1=spark.read.format("json").load("A:\\Spark_output\\jsondata\\part-00000-5b80a6bc-67f1-4661-" +
         "b86c-e201bf46c2da-c000.json").toDF("request","status")
       df1.show()

     }
}