import java.lang.System.setProperty
import java.lang.System._
import org.apache.spark
import scala.collection.JavaConversions._
import scala.collection.convert.wrapAll._
import scala.collection.convert.decorateAll._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SQL {
  def main(args: Array[String]) {

    //println("WELCOME")
    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    //val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)



    // loading the tweetfile

  //  val file = sqlContext.read.format("csv").load("/Users/Navya Gonuguntla/Desktop/survey.csv")

    val file= sqlContext.read.format("com.databricks.spark.csv").load("/Users/Navya Gonuguntla/Desktop/survey.csv")

   val save1= file
      .write.format("com.databricks.spark.csv")
      .save("/Users/Navya Gonuguntla/Desktop/saved")
    file.registerTempTable("survey")

    val query1= sqlContext.sql("select C13 from survey")
    println("Query 1 Executed!")
    // query1.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputfile1")
    query1.show()

  }
}