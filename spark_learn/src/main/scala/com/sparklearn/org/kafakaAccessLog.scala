package com.sparklearn.org

import org.apache.commons.codec.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import Utilities._
import com.sparklearn.org.readFile.setpattern
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.KafkaUtils._

object kafakaAccessLog {
    def main(args:Array[String]):Unit={
      val patr = setpattern()
      val spark=SparkSession.builder.appName("accesslog_using_kafka").master("local[*]").config("spark" +
        ".sql.warehouse.dir","file:///C:/tmp").getOrCreate()
       val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

      /** we need to set the kafkaparams which is a map and also create set for topics list and create a direct stream
        * using kafkautils package
        */
      val kafkaparams=Map("bootstrap.servers" -> "127.0.0.1:9092","key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer","group.id" -> "default")
      val topicset=Set("access_logs")
      val streamdata: InputDStream[ConsumerRecord[String, String]] =
        createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topicset,kafkaparams))

      /** take only the vlaues as the datastream has topic,msg as keyvalue pairs */
      val rawdata=streamdata.map(_.value())
      val schema = new StructType().add("request",org.apache.spark.sql.types.StringType,true).add("number", org.apache.spark.sql.types.IntegerType, true)
      rawdata.foreachRDD((rdd,time)=>{
        val rdd1=rdd.map(x=>{
          val y=patr.matcher(x)
          if (y.matches()){
            val dat=y.group(5).split(" ")(1)
            (dat,1)
          }
          else{
            None
          }



        })
        val rrdd=rdd1.map(x=>{
          val arr=x.toString.replace("(","").replace(")","").split(",")
          Row(arr(0),arr(1).toInt)
        })
        val df1=spark.createDataFrame(rrdd,schema)
        df1.select("request").groupBy("request").count().show()



      })
      ssc.start()
      ssc.awaitTermination()

    }
}
