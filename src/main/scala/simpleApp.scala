import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object SimpleApp {
  def main(args: Array[String]) {
    val file = new File("/mnt/c/Users/Aditya Chatterjee/Desktop/graphx/output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val logFile = "/opt/spark/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sc = new SparkContext(new SparkConf().setAppName("Simple App"))
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    bw.write(s"Lines with a: $numAs, Lines with b: $numBs")
    bw.close()
    spark.stop()
  }
}
