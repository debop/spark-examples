package org.apache.spark.examples

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.slf4j.LoggerFactory

import scala.util.Properties

abstract class AbstractSparkExample extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  private val log = LoggerFactory.getLogger(getClass)

  def getMaster: String = {
    Properties.envOrElse("MASTER", "local")
  }

  def getSparkContext(appName: String): SparkContext = {
    val conf = new SparkConf().setMaster(getMaster).setAppName(appName)
    new SparkContext(conf)
  }

  protected def deleteDirectory(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      log.debug(s"delete directory. path=$path")
      FileUtils.deleteDirectory(dir)
    }
  }
}
