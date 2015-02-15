package com.github.debop.spark.test

import akka.event.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{Matchers, FunSuite}

object SparkTest extends org.scalatest.Tag("com.github.debop.spark.test.SparkTest")

/**
 * AbstractSparkFunSuite
 * @author sunghyouk.bae@gmail.com 15. 2. 15.
 */
trait AbstractSparkFunSuite extends FunSuite with Matchers {

  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param name the name of the test
   * @param silenceSpark true to turn off spark logging
   */
  def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit): Unit = {
    test(name, SparkTest) {
      sc = new SparkContext("local[4]", name)
      try {
        body
      } finally {
        sc.stop()
        sc = null
        System.clearProperty("spark.master.port")
      }
    }
  }
}

object SparkUtil {
  def silenceSpark(): Unit = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map { loggerName =>
      val logger = Logger.getLogger(loggerName)
      val prevLevel = logger.getLevel
      logger.setLevel(level)
      loggerName -> prevLevel
    }.toMap
  }
}
