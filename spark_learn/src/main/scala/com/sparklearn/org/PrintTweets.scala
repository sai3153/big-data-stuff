

package com.sparklearn.org

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

import Utilities._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val conf=new SparkConf().setAppName("tweets").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc, Seconds(1))

    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into RDD's using map()
    val status1 = tweets.map(status => status.getText())
    //val statuses=status1.map(x=>x.length())
    
    // Print out the first ten
    //statuses.print
    //saving the tweets
    var teewtcount:Long=0
    /**statuses.foreachRDD((rdd,batchtime)=>{
      val  tweets=rdd.coalesce(1)
      tweets.saveAsTextFile("c:\\tweets_spark"+batchtime.milliseconds.toString)
      teewtcount+=tweets.count()
      if(teewtcount==100){
        sys.exit(1)
      }

    })*/

    //printing the average twwet length and using aggregate to calculate sum with a seqop,combop functions
    /**
    def seqop(x:Int,y:Int):Int={
      x+y
    }
    def combop(x:Int,y:Int)={
      x+y
    }
    status1.foreachRDD((rdd,time)=>{
      //acc+=rdd.count()
      val rdd1=rdd.map(x=>(1,x.length))
      val tweetc =rdd.count()
      ////val total=rdd1.reduceByKey(_+_)
      //val total1=List(1,2,2)
      println("the total is++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      //val total2=total.map(x=>(x._2))
      val rdd2=rdd1.aggregateByKey(0)(seqop,combop)
      println(s"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@:$tweetc")
      if(tweetc!=0){
        val avg=rdd2.first._2/tweetc
        println(s"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%55:$avg")
      }

      //val avg1=rdd1.reduce((x,y)=>{x+y})
      //print(s"the avg for each second is :::::::: $avg1")
      /**if (acc==20){
        ssc.stop()
        sys.exit(1)
      }*/


    })*/

    //tracking the most popular hashtags .......
    /** split the rdd " " and get the words and filter the words that startwith "#" and count them and find the largest of them naming it popular
     */
    val filstats=status1.flatMap(status=>{status.split(" ")}).filter(status=>status.startsWith("#"))
    val keyvalrdd=filstats.map(x=>{(x,1)})
    val reducerdd=keyvalrdd.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(300),Seconds(1))
    val poptweet=reducerdd.transform(rdd=>rdd.sortByKey(false))
    poptweet.print()
    // Kick it all off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}