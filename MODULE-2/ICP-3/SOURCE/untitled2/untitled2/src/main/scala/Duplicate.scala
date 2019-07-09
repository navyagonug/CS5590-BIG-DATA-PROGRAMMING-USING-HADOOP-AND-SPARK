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

object Duplicate {
  def main(args: Array[String]) {

    //println("WELCOME")
    setProperty("hadoop.home.dir", "C:\\winutils\\")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local").set("com.spark.executor", "   ")
    //val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)



    // loading the tweetfile

    //  val file = sqlContext.read.format("csv").load("/Users/Navya Gonuguntla/Desktop/survey.csv")

    val file = sqlContext.read.format("com.databricks.spark.csv").load("/Users/Navya Gonuguntla/Desktop/survey.csv")


    val f1=file.registerTempTable("survey")
    val f2=file.registerTempTable("survey1")

    val query3 = sqlContext.sql("select COUNT(),C3 from survey GROUP By C3 Having COUNT() > 1")
    query3.show()
    // query3.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputfile4")
  }
}