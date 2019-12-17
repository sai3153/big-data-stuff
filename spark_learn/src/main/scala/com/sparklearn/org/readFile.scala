package com.sparklearn.org

import java.util.regex.Pattern

import org.mortbay.util.ajax.JSON.Source

object readFile {

  def setpattern():Pattern={
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }

  def main(args:Array[String]):Unit={
    import scala.io.Source
    for(i<-Source.fromFile("C:\\Users\\pv sravani\\Desktop\\spark_learn\\access_log.txt").getLines()){
      //println(i)
      val pt1=setpattern()
      val getmatch=pt1.matcher(i)
      if (getmatch.matches()){
        println(getmatch.group(2))
      }

    }
  }

}
