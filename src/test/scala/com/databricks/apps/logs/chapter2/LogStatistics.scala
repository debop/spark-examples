package com.databricks.apps.logs.chapter2

import scala.collection.mutable

/**
 * LogStatistics
 * @author sunghyouk.bae@gmail.com at 15. 2. 26.
 */
case class SizeStats(sum: Long, count: Long, min: Long, max: Long) {
  override def toString: String =
    s"Avg: ${sum / count}, Min: $min, Max: $max"
}

case class LogStatistics(contentSizeStats: SizeStats,
                         responseCodeToCount: Seq[(Int, Long)],
                         ipAddress: Seq[String],
                         topEndPoints: Seq[(String, Long)]) {
  override def toString: String =
    s"""
       |Content Size $contentSizeStats
        |Response code counts: ${responseCodeToCount.mkString(",")}
        |IPAddresses > 10 times: ${ipAddress.mkString(",")}
        |Top Endpoints: ${topEndPoints.mkString(",")}
     """.stripMargin
}

