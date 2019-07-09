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

object Query {
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


    file.registerTempTable("survey")
   val query1= sqlContext.sql("select * from survey where C13 like '%Yes' union select * from survey where C13 like '%No' order by C3")

    println("Union Query Executed!")
    val query2= sqlContext.sql("select C7,count(*) from survey group by C7")
    // println("GROUPBY Query Executed!")
    // query1.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputfile1")
   // query2.map(x=> (x(0),x(1))).coalesce(1,true).saveAsTextFile("outputfile2")
   query1.show()
  //  query2.show()

  }
}