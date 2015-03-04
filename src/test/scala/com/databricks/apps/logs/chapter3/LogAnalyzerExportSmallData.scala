package com.databricks.apps.logs.chapter3

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import com.databricks.apps.logs.ApacheAccessLog
import com.databricks.apps.logs.chapter2.LogAnalyzerRDD
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.sql.SQLContext

/**
 * LogAnalyzerExportSmallData
 * @author sunghyouk.bae@gmail.com at 15. 3. 3.
 */
class LogAnalyzerExportSmallData extends AbstractSparkExample {

  val logFile = "data/apache.access.log"
  val outputFile = "data/log-statistics.txt"

  sparkTest("log analyzer export small data") {

    val sqlContext = new SQLContext(sc)

    val accessLogs = sc.textFile(logFile).map(ApacheAccessLog.parseLogLine)
    val logAnalyzerRDD = new LogAnalyzerRDD(sqlContext)
    val logStatistics = logAnalyzerRDD.processRdd(accessLogs)

    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)))

    out.write(logStatistics.toString)
    out.close()
  }
}
