package org.apache.spark.examples

import breeze.linalg._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

class SparkKMeans extends AbstractSparkExample {

  import SparkKMeans._

  sparkTest("K-Means clustering") {

    val lines: RDD[String] = sc.textFile("data/mllib/kmeans_data.txt")
    val data: RDD[Vector[Double]] = lines.map(parseVector).cache()
    val K = 2
    val convergeDist = 0.001

    val kPoints: Array[Vector[Double]] = data.takeSample(withReplacement = false, K, 42).toArray
    var tempDist = 1.0

    while (tempDist > convergeDist) {
      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closest.reduceByKey {
        case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
      }
      val newPoints = pointStats.map { (pair: (Int, (Vector[Double], Int))) =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))
      }.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }

      println(s"Finished iteration (delta = $tempDist)")
    }

    println("Final centers:")
    kPoints.foreach(println)
  }
}

object SparkKMeans {
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }
}
