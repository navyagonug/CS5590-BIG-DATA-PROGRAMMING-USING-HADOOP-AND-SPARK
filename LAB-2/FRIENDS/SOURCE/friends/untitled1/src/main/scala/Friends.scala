import org.apache.spark._
import org.apache.log4j.{Level, Logger}


object Friends{

  def main(args: Array[String]): Unit = {


    //Controlling log level

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("facefriends").setMaster("local[*]");
    val sc = new SparkContext(conf)

    // Mapper and Reducer functions are defined
    def Fmapper(line: String) = {
      val words = line.split(" ")
      val key = words(0)
      val pairs = words.slice(1, words.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }


    // This is a reducer function that groups data by key.
    // Accumulator is used to intersect the data and find mutual friends
    def Freducer(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set
    }

    // An input file j.txt is given
    val file = sc.textFile("C:\\Users\\saile\\Desktop\\NAVYA\\j.txt")

    val results = file.flatMap(Fmapper)
      .reduceByKey(Freducer)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} , (${line._2.mkString(" ")})")})

    // Saving output as textfile
    results.coalesce(1).saveAsTextFile("Friendsop")

  }

}