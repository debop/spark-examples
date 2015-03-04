package com.databricks.apps.logs.chapter3

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.examples.AbstractSparkExample

/**
 * LogAnalyzerExportRDD
 * @author sunghyouk.bae@gmail.com at 15. 3. 3.
 */
class LogAnalyzerExportRDD extends AbstractSparkExample {

  val NUM_PARTITIONS = 2

  sparkTest("log analayzer export RDD") {
    val inputFile = "data/apache.access.log"
    val outputDirectory = "data/output"

    val accessLogs = sc.textFile(inputFile)
                     .map(ApacheAccessLog.parseLogLine)
                     .repartition(NUM_PARTITIONS)

    accessLogs.saveAsTextFile(outputDirectory)
  }

}
