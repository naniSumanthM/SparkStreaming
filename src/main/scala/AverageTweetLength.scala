import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._
import java.util.concurrent.atomic._

object AverageTweetLength {
  def main(args: Array[String]) {

    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    val statuses = tweets.map(status => status.getText())

    val lengths = statuses.map(status => (status.length))

    var totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)
    var rddMaxVal = new AtomicLong(0)

    lengths.foreachRDD((rdd, time) => {

      val repartition = rdd.repartition(1).cache()
      var count = rdd.count()
      rddMaxVal.set(repartition.max())

      if (count > 0) {
        totalTweets.getAndAdd(count)

        //add all the totals in the dStream containing rdd's that represent the total lenghts of each tweet
        totalChars.getAndAdd(rdd.reduce((x,y) => x + y))

        println("Total tweets: " + totalTweets.get() +
                "Total characters: " + totalChars.get() +
                "Average: " + totalChars.get() / totalTweets.get() +
                "Longest tweet in RDD: " + rddMaxVal.get())
      }
    })

    ssc.checkpoint("/Users/sumanth/Documents/BigData/Courses/SparkScala/checkpointData/")
    ssc.start()
    ssc.awaitTermination()
  }  
}