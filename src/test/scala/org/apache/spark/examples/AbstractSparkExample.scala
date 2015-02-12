package org.apache.spark.examples

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.slf4j.LoggerFactory

abstract class AbstractSparkExample extends FunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  private val log = LoggerFactory.getLogger(getClass)

  protected def deleteDirectory(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      log.debug(s"delete directory. path=$path")
      FileUtils.deleteDirectory(dir)
    }
  }
}
