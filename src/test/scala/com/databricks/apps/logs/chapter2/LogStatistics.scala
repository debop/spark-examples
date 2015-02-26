package com.databricks.apps.logs.chapter2

/**
 * LogStatistics
 * @author sunghyouk.bae@gmail.com at 15. 2. 26.
 */
case class SizeStats(sum: Long, count: Long, min: Long, max: Long)

case class LogStatistics(contentSizeStats: SizeStats,
                         responseCodeToCount: List[(Integer, Long)],
                         ipAddress: List[String],
                         topEndPoints: List[(String, Long)])
