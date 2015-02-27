package com.databricks.apps.logs.chapter2

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

/**
 * LogAnalyzerRDD
 * @author sunghyouk.bae@gmail.com at 15. 2. 26.
 */
class LogAnalyzerRDD(val sqlContext: SQLContext) {

  private val log = LoggerFactory.getLogger(getClass)

  import sqlContext._

  def processRdd(accessLogs: RDD[ApacheAccessLog]): LogStatistics = {

    log.debug(s"regist access log to temp table...")

    accessLogs.registerTempTable("logs")
    sqlContext.cacheTable("logs")

    log.debug(s"calc content size...")
    val contentSizeStats =
      sql(
           s"""
              |SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize)
              |FROM logs
         """.stripMargin)
      .map(r => SizeStats(r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3)))
      .first()

    log.debug(s"calc count by response code")
    val responseCodeToCount =
      sql(
           """
             |SELECT responseCode, COUNT(*)
             |FROM logs
             |GROUP BY responseCode
           """.stripMargin)
      .map(r => (r.getInt(0), r.getLong(1)))
      .take(1000)

    log.debug(s"get most reqeusted ip address")
    val ipAddresses =
      sql(
           """
             |SELECT ipAddress, COUNT(*) AS total
             |FROM logs
             |GROUP BY ipAddress
             |HAVING total > 10
           """.stripMargin)
      .map(r => r.getString(0))
      .take(100)

    log.debug(s"get top 10 endpoint")

    val topEndpoints =
      sql(
           """
             |SELECT endpoint, COUNT(*) AS total
             |FROM logs
             |GROUP BY endpoint
             |ORDER BY total DESC
             |LIMIT 10
           """.stripMargin)
      .map(r => (r.getString(0), r.getLong(1)))
      .collect()

    LogStatistics(contentSizeStats, responseCodeToCount, ipAddresses, topEndpoints)
  }
}
