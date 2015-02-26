package com.databricks.apps.logs.chapter1

import com.databricks.apps.logs.{OrderingUtils, ApacheAccessLog}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.SparkContext._

/**
 * The LogAnalyzerStreaming illustrates how to use logs with Spark Streaming to
 * compute statistics every slide_interval for the last window length of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 * % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to, you can add test lines using this command:
 * % cat ../../data/apache.access.log >> [YOUR_LOG_FILE]
 *
 * Example command to run:
 * % spark-submit
 * --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreaming"
 * --master local[4]
 * target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */
class LogAnalyzerStreaming extends AbstractSparkExample {

  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)


  sparkTest("Log Analyzer Streaming") {
    sc.stop()

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Log Analyzer Streaming in Scala")
    sc = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()
    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    windowDStream.foreachRDD { (accessLogs: RDD[ApacheAccessLog]) =>
      if (accessLogs.count() == 0) {
        println(s"No access com.databricks.app.logs received in this time interval")
      } else {
        // Calculate statistics based on the content size.
        val contentSizes = accessLogs.map(log => log.contentSize).cache()
        val sum = contentSizes.reduce(_ + _)
        val count = contentSizes.count()
        val min = contentSizes.min()
        val max = contentSizes.max()
        println(s"Content Size Count: $count, Avg: ${sum / count}, Min: $min, Max: $max")

        // Compute Response Code to Count.
        val responseCodeToCount = accessLogs
                                  .map(log => (log.responseCode, 1))
                                  .reduceByKey(_ + _)
                                  .take(100)

        println(s"Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}")

        // Any IPAddress that has accessed the server more than 10 times.
        val ipAddresses = accessLogs
                          .map(log => (log.ipAddress, 1))
                          .reduceByKey(_ + _)
                          .filter(_._2 > 10)
                          .map(_._1)
                          .take(100)

        println(s"IPAddresses > 10 times: ${ipAddresses.mkString(",")}")

        // Top Endpoints.
        val topEndpoints = accessLogs
                           .map(log => (log.endpoint, 1))
                           .reduceByKey(_ + _)
                           .top(10)(OrderingUtils.SecondValueOrdering)

        println(s"Top Endpoints: ${topEndpoints.mkString(",")}")
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
