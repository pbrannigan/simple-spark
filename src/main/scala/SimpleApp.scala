

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.apache.spark.util.StatCounter
import java.util.ArrayList

/* @author patrick
 */
object SimpleApp {

  def main(args: Array[String]) {
    /*
     * spark://192.168.42.40:7077
     * spark://developer-appliance:7077
     * spark://192.168.42.134:7077
      .set("spark.driver.host", "192.168.42.40")
      .set("spark.driver.port", "7077")
     * 
     */
    val conf = new SparkConf()
      .setMaster("spark://developer-appliance:7077")
      .setAppName("ETL-2")
      .set("spark.akka.heartbeat.interval", "100")
      
    val sc = new SparkContext(conf)
    sc.addJar("/home/patrick/simple-spark/target/scala-2.10/simple-spark_2.10-1.0.jar")
    try {
      val list = List(199000, 200000, 350000, 375000, 500000, 460000, 370000)
      val rdd = sc.parallelize(list)
      val stats = rdd.map(i => MedianStatCounter(i)).reduce(_ merge _)
      println("************************************************************************************")
      println("stats: " + stats)
      println("************************************************************************************")
      Logger.getLogger("INFO").info("stats: " + stats)
    }
    finally {
      sc.stop()
    }
  }
}

    class MedianStatCounter extends Serializable {
      val stats: StatCounter = new StatCounter()
      val median: scala.collection.mutable.MutableList[Int] = new scala.collection.mutable.MutableList[Int]

      def add(x: Int): MedianStatCounter = {
        median += x
        stats merge x
        this
      }

      def merge(other: MedianStatCounter): MedianStatCounter = {
        stats merge other.stats
        median ++= other.median
        this
      }

      private def getMedian: Int = {
        val mid = median.size / 2
        median.sorted.get(mid) match {
          case Some(x) => x
          case None => 0
        }
      }

      override def toString = {
        "stats: " + stats.toString + " median: " + getMedian
      }
    }

    object MedianStatCounter extends Serializable {
      def apply(x: Int) = new MedianStatCounter().add(x)
    }
