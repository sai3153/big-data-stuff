package com.sparklearn.org

import readFile.setpattern
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._


object sql_stream_analysis {
  /**val line:List[String]=List()
  val tpl=Tuple2
  def castthem(x:Row): List[String]={
      for(word <- x){
        line:+x
      }
    line
  }*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("loganalyzer").setMaster("local[*]").set("spark.sql.warehouse.dir", "file:///C:/tmp")

    val sc = new SparkContext(conf)
    //val sqlc=new SQLContext(sc)
    val spark = SparkSession.builder().appName("log analysis").master("Local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val patr = setpattern()
    val logfiles = ssc.socketTextStream("127.0.0.1", 9999).persist(StorageLevel.MEMORY_AND_DISK)
    val rdd1 = logfiles.transform(rdd=>rdd.map(x => {
      val y = patr.matcher(x)
      if (y.matches()) {
        val request1 = y.group(5).split(" ")
        val request=request1(1)

        val status = y.group(6)
        println(request)
        println(status)
        if (request.toString.length != 0 && status.toString == "200") {
          (request,status)
        }
        else {
          ("error", "error")
        }


      }
    }))



    val schema = new StructType().add("request",org.apache.spark.sql.types.StringType,true).add("status", org.apache.spark.sql.types.StringType, false)


    rdd1.foreachRDD((rdd,time)=> {

      val rdd3=rdd.map(x=>{
        (x.toString)
      })
     val rdd4= rdd3.map(x=>{
        val arr=x.replace("(","").replace(")","").split(",")
        Row(arr(0),arr(1))
      })
      val df1=spark.createDataFrame(rdd4,schema)
      df1.createOrReplaceTempView("logtable")
      val df2=spark.sql("select * from logtable")
      df2.write.format("json").mode("append").save("A:\\Spark_output\\jsondata")
    })

    case class Record1(request: String, status: Int)
    ssc.start()
    ssc.awaitTermination()
  }



}


