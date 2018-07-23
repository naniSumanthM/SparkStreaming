import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._

object PopularHashtags {
  def main(args: Array[String]) {
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
  
    setupLogging()
    val tweets = TwitterUtils.createStream(ssc, None)
  
    val statuses = tweets.map(status => status.getText(    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetwords.filter(word => word.artsWith("#"))
    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow((x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))

    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    // Print the top 10
    sortedResults.print

    ssc.checkpoint("/Users/sumanth/Documents/BigData/Courses/SparkScala/checkpointData/")
    ssc.start()
    ssc.awaitTermination()
  }  
}