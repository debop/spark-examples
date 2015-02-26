package com.databricks.apps.logs

import org.slf4j.LoggerFactory

/**
 * Web Server Log 정보
 * @author sunghyouk.bae@gmail.com at 15. 2. 26.
 */
case class ApacheAccessLog(ipAddress: String,
                           clientIdentd: String,
                           userId: String,
                           dateTime: String,
                           method: String,
                           endpoint: String,
                           protocol: String,
                           responseCode: Int,
                           contentSize: Long)

/**
 * Web Server Log를 분석하는 Parser 입니다.
 */
object ApacheAccessLog {

  private val log = LoggerFactory.getLogger(getClass)
  lazy val EmptyLog = ApacheAccessLog("", "", "", "", "", "", "", 0, 0)

  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)""".r

  def parseLogLine(logLine: String): ApacheAccessLog = {
    val res = PATTERN.findFirstMatchIn(logLine)
    if (res.isEmpty) {
      log.warn(s"Cannot parse log line: $logLine")
      EmptyLog
    }
    else {
      val m = res.get
      val contentSize = if (m.group(9) == "-") 0L else m.group(9).toLong
      ApacheAccessLog(m.group(1),
                       m.group(2),
                       m.group(3),
                       m.group(4),
                       m.group(5),
                       m.group(6),
                       m.group(7),
                       m.group(8).toInt,
                       contentSize)
    }
  }
}
