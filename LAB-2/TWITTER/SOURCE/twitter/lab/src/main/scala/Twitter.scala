import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.twitter._
import org.apache.spark.{ SparkContext, SparkConf }

object Twitter {

  val conf = new SparkConf().setMaster("local[4]").setAppName("Tweets Extraction")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // The system properties are set below giving the authorization keys ad tokens

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Spark streaming context is set with a time interval of 3 seconds
    val sc1 = new StreamingContext(sc, Seconds(3))
    val stream = TwitterUtils.createStream(sc1, None, filters)

    // Split the stream on space and extract those words that starts with #(hashtags) with a filter
    val words = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Count each word in each and every set
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated to the console
    wordCounts.print()

    sc1.start()
    sc1.awaitTermination()

  }

}