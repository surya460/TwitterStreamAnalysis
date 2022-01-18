package org.tweet.stream



import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.countDistinct



object StreamAnalysis {

  // Functionality to handle, parse and persist Tweets Status JSON

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //Input Arguments Parsing

    if (args.length < 4) {
      System.err.println("Usage: StreamAnalysis <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")

      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val keyWord = args.takeRight(args.length - 4).mkString(" ")


    //Spark Configuration setup for Spark Session and Streaming Context
    val sparkConf = new SparkConf().setAppName("getTweets").setMaster("local[3]")
    val spark = SparkSession
      .builder()
      .config("spark.master", "local[2]")
      .appName("interfacing spark sql to hive metastore through thrift url below")
      .config("hive.metastore.uris", "thrift://localhost:10000")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sc=spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(1))


    //Configuration to authenticate and connect to Twitter API
    System.setProperty("twitter4j.oauth.consumerKey",consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Filter to fetch certain tweets

    //val seq:Seq[String] = Seq("Justin Bieber")
    val seq:Seq[String] = Seq(keyWord)

    //Create Stream to Stream from Twitter API and filter tweets based on key word
    println("Creating the DStream with the search Keyword: " + keyWord)
    val rawtweets = TwitterUtils.createStream(ssc, None,seq)




    //Parse logic to fetch required fields. We can also use JSON parse logic in case all fields are required. To avoid overhead parsed only the required fields
    val parsed_tweets = rawtweets.map(each_tweet => (each_tweet.getId,each_tweet.getText,each_tweet.getRetweetCount) )


    //Filter logic to fetch tweets related to music. This can also be handled at DataFrame logic.
    val filetered_tweets=parsed_tweets.filter(text=>text._2.toLowerCase.contains("music"))



    //Iterate through each DStreamed RDD and then covert the RDD to Dataframe for data enrichment or persistence logic.
    filetered_tweets.foreachRDD{ rdd =>
      //Validating if RDD is empty to avoid unnecessary processing
      if(!rdd.isEmpty) {
        //RDD to Dataframe conversion logic
       val tweetsDataFrame = rdd.toDF("id","text","retweetcount")
        //Registering the DF as temp view to query using Spark SQL
        tweetsDataFrame.createOrReplaceTempView("tweetsDF")

        //SQL to remove new line and special characters from the text - tweet data
        val outputdf = spark.sql("select id,regexp_replace(text, \"[\\\\r\\\\n]\", \"\") as text,retweetcount,from_unixtime(unix_timestamp()) as insert_ts from tweetsDF where lower(text) like '%music%' and retweetcount=0")

        //Cache logic to run different operations on the same dataframe
        outputdf.cache()

        //Aggreate and filter operations
        outputdf.show()
        val totalcount = outputdf.count()
        val totalDistinctCount = outputdf.agg(countDistinct("id")).alias('distinctCount).collect()(0).getLong(0)


        println("Count of tweets: " + totalcount)
        println("Count of Distinct tweets: " + totalDistinctCount)

        //Persistence to Hive; Delta Table will be optimal for this solution to avoid small files

        outputdf.write.format("parquet").mode(SaveMode.Append).saveAsTable("enrich.tweets")

      }
    }
    //Checkpointing for Stateful Processing
    ssc.checkpoint("/Users/Surya_P")
    ssc.start()
    ssc.awaitTermination()
  }
}