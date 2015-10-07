

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.apache.spark.util.StatCounter
import java.util.ArrayList

/* @author patrick
 */
object SimpleApp {

  def main(args: Array[String]) {

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

      val offices = List((1, 199000), (3, 200000), (1, 350000), (3, 375000), (3, 500000), (1, 460000), (1, 370000))
      val officeRdd = sc.parallelize(offices)
      val officeStats = officeRdd.map(i => (i._1, MedianStatCounter(i._2))).reduceByKey(_ merge _).collect()
      println("************************************************************************************")
      officeStats.foreach(println)
      println("************************************************************************************")

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val jdbcDF = sqlContext.load("jdbc", Map(
        "url" -> "jdbc:oracle:thin:@dbnew.dev.terradatum.com:1521:METRICB",
        "driver" -> "oracle.jdbc.driver.OracleDriver",
        "user" -> "www_app",
        "password" -> "s3rvic3s",
        "dbtable" -> "\"MLS\""
        ))

      val show = jdbcDF.groupBy("SUPPORTS_NEW_CONSTRUCTION").count().show()

      val license = jdbcDF.groupBy("LICENSOR").count().show()

      val modified = jdbcDF.map(r => (r.getAs[String](5), 1)).reduceByKey(_ + _).collect()
      println("************************************************************************************")
      modified.foreach(println)
      println("************************************************************************************")
    }
    finally {
      sc.stop()
    }
  }
}

    class MedianStatCounter extends Serializable {
      val stats: StatCounter = new StatCounter()
      val median: scala.collection.mutable.MutableList[Int] = new scala.collection.mutable.MutableList[Int]
      var total = 0;

      def add(x: Int): MedianStatCounter = {
        total += x
        median += x
        stats merge x
        this
      }

      def merge(other: MedianStatCounter): MedianStatCounter = {
        total += other.total
        median ++= other.median
        stats merge other.stats
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
        "stats: " + stats.toString + " total: " + total + " median: " + getMedian
      }
    }

    object MedianStatCounter extends Serializable {
      def apply(x: Int) = new MedianStatCounter().add(x)
    }
