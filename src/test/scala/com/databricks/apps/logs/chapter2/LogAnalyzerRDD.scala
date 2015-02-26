package com.databricks.apps.logs.chapter2

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * LogAnalyzerRDD
 * @author sunghyouk.bae@gmail.com at 15. 2. 26.
 */
class LogAnalyzerRDD(val sqlContext: SQLContext) {

  def processRdd(accessLogs: RDD[ApacheAccessLog]): LogStatistics = {

  }
}
