package org.apache.spark.examples

import breeze.linalg.{DenseVector, Vector, squaredDistance}

import scala.collection.mutable
import scala.util.Random

/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.mllib.clustering.KMeans
 */
class LocalKMeansFunSuite extends AbstractSparkExample {

  val N = 1000
  // Scaling factor
  val R = 1000
  val D = 10
  val K = 10
  val convergeDist = 0.001
  val rand = new Random(42)

  def generateData = {
    def generatePoint(i: Int) = {
      DenseVector.fill(D) {rand.nextDouble() * R}
    }
    Array.tabulate(N)(generatePoint)
  }

  def closestPoint(p: Vector[Double], centers: mutable.HashMap[Int, Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 1 to centers.size) {
      val vCurr = centers.get(i).get
      val tempDist = squaredDistance(p, vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  test("K-means clustering") {
    val data: Array[DenseVector[Double]] = generateData
    val points = new mutable.HashSet[Vector[Double]]
    val kPoints = new mutable.HashMap[Int, Vector[Double]]
    var tempDist = 1.0

    while (points.size < K) {
      points.add(data(rand.nextInt(N)))
    }

    val iter = points.iterator
    for (i <- 1 to points.size) {
      kPoints.put(i, iter.next())
    }

    println("Initial centers: " + kPoints)

    while (tempDist > convergeDist) {
      val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      val mappings = closest.groupBy[Int](x => x._1)

      val pointStats = mappings.map { (pair: (Int, Array[(Int, (DenseVector[Double], Int))])) =>
        pair._2.reduceLeft[(Int, (Vector[Double], Int))] {
          case ((id1, (x1, y1)), (id2, (x2, y2))) => (id1, (x1 + x2, y1 + y2))
        }
      }

      val newPoints = pointStats.map { (mapping: (Int, (Vector[Double], Int))) =>
        (mapping._1, mapping._2._1 * (1.0 / mapping._2._2))
      }

      tempDist = 0.0
      for (mapping <- newPoints) {
        tempDist += squaredDistance(kPoints.get(mapping._1).get, mapping._2)
      }

      for (newP <- newPoints) {
        kPoints.put(newP._1, newP._2)
      }
    }

    println("Final centers: " + kPoints)
  }

}
