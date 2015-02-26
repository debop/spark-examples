package com.databricks.apps.logs.chapter1

import com.databricks.apps.logs.{ApacheAccessLog, OrderingUtils}
import org.apache.spark.SparkContext._
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * The LogAnalyzer takes in an apache access log file and
 * computes some statistics on them.
 */
class LogAnalyzer extends AbstractSparkExample {

  val logFile = "data/apache.access.log"

  sparkTest("Log Analyzer") {

    val accessLogs = sc.textFile(logFile).map(line => ApacheAccessLog.parseLogLine(line)).cache()

    // Calculate statistics based on the content size.
    val contentSizes = accessLogs.map(log => log.contentSize).cache()

    println(s"Log Count: ${contentSizes.count()}")

    val contentSizeAvg = contentSizes.reduce(_ + _) / contentSizes.count()
    val contentSizeMin = contentSizes.min()
    val contentSizeMax = contentSizes.max()
    println(s"Content Size Avg: $contentSizeAvg, Min: $contentSizeMin, Max: $contentSizeMax")

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

  sparkTest("Log Analyzer with SQL") {

    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()
    val accessTableName = "webLogs"
    accessLogs.registerTempTable(accessTableName)

    // Calculate statistics based on the content size.
    val contentSizeStats = sql(s"SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM $accessTableName")
                           .first()
    val sum = contentSizeStats.getLong(0)
    val count = contentSizeStats.getLong(1)
    val min = contentSizeStats.getLong(2)
    val max = contentSizeStats.getLong(3)
    println(s"Content Size Avg: ${sum / count}, Min: $min, Max: $max")

    // Compute Response Code to Count.
    val responseCodeToCount = sql(s"SELECT responseCode, COUNT(*) FROM $accessTableName GROUP BY responseCode ORDER BY responseCode LIMIT 1000")
                              .map(row => (row.getInt(0), row.getLong(1)))
                              .collect()

    println(s"Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}")

    // Any IPAddress that has accessed the server more than 10 times.
    val ipAddresses = sql(s"SELECT ipAddress, COUNT(*) as total FROM $accessTableName GROUP BY ipAddress HAVING total > 10 LIMIT 1000")
                      .map(row => row.getString(0))
                      .collect()

    println(s"IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}")

    val topEndpoints = sql(s"SELECT endpoint, COUNT(*) as total FROM $accessTableName GROUP BY endpoint ORDER BY total DESC LIMIT 10")
                       .map(row => (row.getString(0), row.getLong(1)))
                       .collect()

    println(s"Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}")
  }


}
