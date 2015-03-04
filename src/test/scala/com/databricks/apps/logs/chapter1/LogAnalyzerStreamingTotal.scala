package com.databricks.apps.logs.chapter1

import java.util.concurrent.atomic.AtomicLong

import breeze.linalg.{max, min}
import com.databricks.apps.logs.{ApacheAccessLog, OrderingUtils}
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * This LogAnalyzerStreaming program reads the localhost 9999 socket
 * for incoming Apache access logs and computes the statistics on those logs
 * every 10 seconds.  It computes the statistics for data from all of time.
 *
 * To feed the new lines of some logfile into a socket, run this command:
 * % tail -f [YOUR_LOG_FILE] | nc -lk 9999
 *
 * If you don't have a live log file that is being written to,
 * you can add test lines using this command:
 * % cat data/apache.access.log >> [YOUR_LOG_FILE]
 *
 * Example command to run:
 * % spark-submit
 * --class "com.databricks.apps.logs.chapter1.LogAnalyzerStreaming"
 * --master local[4]
 * target/scala-2.10/spark-logs-analyzer_2.10-1.0.jar
 */

object LogAnalyzerStreamingTotal {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(Long.MaxValue)
  val runningMax = new AtomicLong(Long.MaxValue)
}

class LogAnalyzerStreamingTotal extends AbstractSparkExample {

  import com.databricks.apps.logs.chapter1.LogAnalyzerStreamingTotal._

  sparkTest("Log analyzer streaming total") {

    val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL)

    // NOTE: Checkingpointing must be enabled to use updateStateByKey
    streamingContext.checkpoint("/tmp/log-analyzer-stream-total-scala")

    val logLinesDStream = streamingContext.socketTextStream("localhost", 9999)

    val accessLogsDStream = logLinesDStream.map(ApacheAccessLog.parseLogLine).cache()

    val contentSizesDStream = accessLogsDStream.map(log => log.contentSize).cache()
    contentSizesDStream.foreachRDD { rdd =>
      val count = rdd.count()
      if (count > 0) {
        runningSum.getAndAdd(rdd.reduce(_ + _))
        runningCount.getAndAdd(count)
        runningMin.set(min(runningMin.get, rdd.min()))
        runningMax.set(max(runningMax.get, rdd.max()))
      }
      if (runningCount.get() == 0) {
        println(s"Cotent Size Avg: -, Min: -, Max: -")
      } else {
        println(s"Content Size Avg: ${runningSum.get() / runningCount.get()}, Min: ${runningMin.get}, Max: ${runningMax.get}")
      }
    }

    val cumulativeResponseCodeCountDStream = accessLogsDStream
                                             .map(log => (log.responseCode, 1L))
                                             .reduceByKey(_ + _)
                                             .updateStateByKey(computeRunningSum)

    cumulativeResponseCodeCountDStream.foreachRDD { (rdd: RDD[(Int, Long)]) =>
      val responseCodeToCount = rdd.take(100)
      println(s"Response code counts: ${responseCodeToCount.mkString(",")}")
    }

    val ipAddressDStream = accessLogsDStream
                           .map(log => (log.ipAddress, 1L))
                           .reduceByKey(_ + _)
                           .updateStateByKey(computeRunningSum)
                           .filter(_._2 > 10)
                           .map(_._1)

    ipAddressDStream.foreachRDD { rdd =>
      val ipAddresses = rdd.take(100)
      println(s"IPAddresses > 10 times: ${ipAddresses.mkString(",")}")
    }

    val endpointCountsDStream = accessLogsDStream
                                .map(log => (log.endpoint, 1L))
                                .reduceByKey(_ + _)
                                .updateStateByKey(computeRunningSum)

    endpointCountsDStream.foreachRDD { rdd =>
      val topEndpoints = rdd.top(10)(OrderingUtils.SecondValueLongOrdering)
      println(s"Top Endpoints: ${topEndpoints.mkString(",")}")
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
