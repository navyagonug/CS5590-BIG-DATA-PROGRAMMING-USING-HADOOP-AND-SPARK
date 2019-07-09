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

object Row {
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


    file.registerTempTable("survey")
    val row = file.rdd.take(13).last
    print(row)

    // val query5=sqlContext.sql("select q3.C3,q3.C4,q4.C5 from query3 q3 join query4 q4 on q3.C3=q4.C3")
    //query5.show()
  }
}