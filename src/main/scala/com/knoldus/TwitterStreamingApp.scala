package com.knoldus

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStreamingApp extends App {

  val sparkConf = new SparkConf().setMaster("local[1]").setAppName("Streaming Twitter")
  val streamingContext = new StreamingContext(sparkConf, Seconds(2))
  val bufferedReader = new BufferedReader(new InputStreamReader(System.in))

  println("Please Enter Twitter Consumer Key ")
  val CONSUMER_KEY = bufferedReader.readLine()
  println("Please Enter Twitter Consumer Secret")
  val CONSUMER_SECRET = bufferedReader.readLine()
  println("Please Enter Twitter Access Token ")
  val ACCESS_TOKEN = bufferedReader.readLine()
  println("Please Enter Twitter Access Token Secret")
  val ACCESS_TOKEN_SECRET = bufferedReader.readLine()

  System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY)
  System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET)
  System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN)
  System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET)


  val stream = TwitterUtils.createStream(streamingContext, None).window(Seconds(4))

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  hashTags.saveAsTextFiles("/home/User/Tweets")

  streamingContext.start()
  streamingContext.awaitTermination()
}
