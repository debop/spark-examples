package org.apache.spark.examples

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

class SparkPi extends AbstractSparkExample {

  test("calc pi") {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)

    val slices = 2
    val n = math.min(500000 * slices, Int.MaxValue).toInt
    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1.0) 1 else 0
    }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n.toDouble)

    sc.stop()
  }

  test("calc pi with Disk save") {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local")
    val sc = new SparkContext(conf)

    val slices = 2
    val n = math.min(500000 * slices, Int.MaxValue).toInt

    val rdd = sc.parallelize(1 to n, slices)
    rdd.persist(StorageLevel.DISK_ONLY)

    val count = rdd.map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1.0) 1 else 0
    }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n.toDouble)

    sc.stop()
  }

}
