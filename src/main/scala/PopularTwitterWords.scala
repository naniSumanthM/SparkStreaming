import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._

object PopularTwitterWords {

  def main(args: Array[String]) {

    setupTwitter()

    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))

    setupLogging()
    
    // DStream contains a collection of rdd, which are in turn row objects of data
    val tweets = TwitterUtils.createStream(ssc, None)

    val mostPopularTweetWords = {
        tweets.map(status => status.getText())
              .flatMap(tweetText => tweetText.split(" "))
              .filter(tweetText => !tweetText.startsWith("@") && !tweetText.startsWith("#"))
              .map(tweetText => (tweetText, 1))
              .reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
              .transform(rdd => rdd.sortBy(x => x._2, false))
              .print()
    }

    ssc.checkpoint("/Users/sumanth/Documents/BigData/Courses/SparkScala/checkpointData/")
    ssc.start()
    ssc.awaitTermination()
  }
}