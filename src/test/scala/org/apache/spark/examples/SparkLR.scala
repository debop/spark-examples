package org.apache.spark.examples

import breeze.linalg.{DenseVector, Vector}
import breeze.numerics.exp
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

class SparkLR extends AbstractSparkExample {

  import SparkLR._

  test("Spark Logistic regression based classification") {
    val conf = new SparkConf().setMaster("local").setAppName("SparkLR")
    val sc = new SparkContext(conf)

    val points = sc.parallelize(generateData).cache()

    var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println(s"Initial w: $w")

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient: Vector[Double] = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * w.dot(p.x))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println(s"Final w: $w")

    sc.stop()
  }
}

/**
 * Logistic regression based classification.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
 * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
 */
object SparkLR {

  // number of data points
  val N = 10000
  // number of dimension
  val D = 10
  // Scaling factor
  val R = 0.7

  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D) {rand.nextGaussian() + y + R}
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

}
