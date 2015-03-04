package com.databricks.apps.logs.chapter2

import com.databricks.apps.logs.ApacheAccessLog
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * 특정 Directory 를 모니터링해서 파일이 추가되면 읽어드려 정보를 관리한다.
 * 테스트 프로그램을 실행하고, terminal에서 data/logs 에 log 파일을 복사해 넣으면 시작된다.
 *
 * cp apache.access.log apache.access-1.log
 *
 * @author sunghyouk.bae@gmail.com at 15. 3. 3.
 */
class LogAnalyzerBatchImportDirectory extends AbstractSparkExample {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  sparkTest("log analyzer batch import in directory") {
    val ssc = new StreamingContext(sc, SLIDE_INTERVAL)
    val sqlContext = new SQLContext(sc)

    val directory = "data/logs/"

    // This methods monitors a directory for new files to read in for streaming.
    val accessLogsDStream = ssc.textFileStream(directory).map(ApacheAccessLog.parseLogLine).cache()

    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    val logAnalyzerRDD = new LogAnalyzerRDD(sqlContext)

    windowDStream.foreachRDD { accessLogs =>
      if (accessLogs.count() == 0) {
        println(s"No access logs in this time interval.")
      } else {
        val logStatistics = logAnalyzerRDD.processRdd(accessLogs)
        println(logStatistics)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
