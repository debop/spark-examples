package com.databricks.apps.logs.chapter2

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * LogAnalyzerBatchImport
 * @author sunghyouk.bae@gmail.com at 15. 2. 27.
 */
class LogAnalyzerBatchImport extends AbstractSparkExample {

  val logFile = "data/apache.access.log"

  sparkTest("log analyzer by batch import") {
    val sqlContext = new SQLContext(sc)

    val accessLogs: RDD[ApacheAccessLog] = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine).cache()

    val logAnalyzerRDD = new LogAnalyzerRDD(sqlContext)
    val logStatistics = logAnalyzerRDD.processRdd(accessLogs)

    println(logStatistics)
  }

}
