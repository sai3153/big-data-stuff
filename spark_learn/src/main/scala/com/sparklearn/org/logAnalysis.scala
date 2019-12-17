package com.sparklearn.org

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import readFile.setpattern

object logAnalysis {

  def sparkinit():StreamingContext={


    val conf = new SparkConf().setAppName("loganalyzer").setMaster("local[*]").set("spark.sql.warehouse.dir", "file:///C:/tmp")

    val sc = new SparkContext(conf)
    val sqlc=new SQLContext(sc)
    val spark=SparkSession.builder().appName("log analysis").master("Local[*]").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    ssc
  }

  def highestclicked_sites():Unit={
    val patr=setpattern()
    val conf=new SparkConf().setAppName("loganalyzer").setMaster("local[*]")

    val sc=new SparkContext(conf)
    val ssc= new StreamingContext(sc,Seconds(1))
    val logfiles=ssc.socketTextStream("127.0.0.1",9999).persist(StorageLevel.MEMORY_AND_DISK)
    //logfiles.foreachRDD((rdd,time)=>{rdd.foreach(println)})
    val res1=logfiles.transform(rdd=>rdd.map(x=>{val y=patr.matcher(x)
      if(y.matches()) {
        //println("the group is")
        //println(y.group(5))
        y.group(5)
      }
    }))
    val res2=res1.map(x=>{val arr=x.toString.split(" ")
      if (arr.length==3){
        (arr(1))
      }
      else{
        ("error")
      }
    })

    val res3=res2.map(x=>(x,1)).reduceByKeyAndWindow((a:Int,y:Int)=>(a+y),Seconds(500),Seconds(2))
    res3.foreachRDD((rdd,time)=>{
      val finalz=rdd.sortBy(x=>x._2,false)
      finalz.foreach(println)


    })
    ssc.start()
    ssc.awaitTermination()

  }

  def getalarm():Unit={

   val patr=setpattern()
    val conf=new SparkConf().setAppName("loganalyzer").setMaster("local[*]")

    val sc=new SparkContext(conf)
    val ssc= new StreamingContext(sc,Seconds(1))


    val logfiles=ssc.socketTextStream("127.0.0.1",9999).persist(StorageLevel.MEMORY_AND_DISK)
    val res1=logfiles.transform(rdd=>rdd.map(x=>{val y=patr.matcher(x)
      if(y.matches()) {
        //println("the group is")
        //println(y.group(5))
        val str=y.group(6).toString()
        if(str.toString.length==0 || str.toString() !="200"){
          (x,"error")

        }

      }

    }))
    res1.foreachRDD((rdd,time)=>{
      println("*********errorsssssssssss*8888888888888888888888888888")
      rdd.foreach(println)
    })
    //val res2=res1.count()
    //res2.print()
   // if (res2.foreachRDD((rdd,time)=>{if(rdd.count()>0){println("*********errorsssssssssss*8888888888888888888888888888");sys.exit(1)}})>0){
    ssc.start()
    ssc.awaitTermination()
  }

  def trigger_alarm():Unit= {
    val patr = setpattern()
    val scc = sparkinit()
    /**val conf = new SparkConf().setAppName("loganalyzer").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))*/

    val logfiles = scc.socketTextStream("127.0.0.1", 9999).persist(StorageLevel.MEMORY_AND_DISK)
    val logcnt=logfiles.count()
    if (logcnt!=0) {
      val res1 = logfiles.transform(rdd => rdd.map(x => {
        val y = patr.matcher(x)

        if (y.matches()) {
          val str = y.group(6).toString()
          if (str.toString.length == 0 || str.toString() != "200") {
            ("error")

          }
          else {
            if (str.toString == "200") {
              ("success")
            }
          }

        }
        else {
          ("error")
        }
      }))

      val res2 = res1.map(x => (x, 1)).reduceByKeyAndWindow((x: Int, y: Int) => (x + y), Seconds(300), Seconds(1))
      // val err=res2.filter(x=>x._1=="error")
      res2.foreachRDD((rdd, time) => {
        val cnt = rdd.count()
        if (cnt != 0) {
          val err = rdd.filter(x => x._1 == "error")
            val cerr=err.count()
          val suc = rdd.filter(x => x._1 == "success")

             val csuc=suc.count()

          if (cerr.toInt >0 && csuc.toInt>0) {
            //println(s"the error count is $cerr and sucess count is $csuc")
            if(err.first()._2.toInt > suc.first()._2.toInt){
              val e1=err.first()._2.toInt
              val e2=suc.first()._2.toInt
            println(s"*************************************$e1***********$e2*******************************" +
              "*********************8ERRORERORERROREROROR**********************/888888888888888888888888888888")
          }}
        }
      })
      //val suc=res2.filter(x=>x._1=="success").count()
      scc.start()
      scc.awaitTermination()

    }
  }
  def main(args: Array[String]): Unit = {

    /**gives  the number of highest viewed websites */
    //highestclicked_sites()
    /** this gives all the error records .*/
    //getalarm()
    /** this gives a trigger when errors are more than success logs*/
    trigger_alarm()


    }


}
