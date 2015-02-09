package org.apache.spark.examples

import scala.util.Random
import breeze.linalg._

class LocalFunSuite extends AbstractSparkExample {

  test("Logistic regression based classification") {
    val N = 10000 // number of data points
    val D = 10    // number of dimension
    val R = 0.7   // Scaling factor
    val ITERATIONS = 5
    val rand = new Random(42)

    case class DataPoint(x:Vector[Double], y:Double)

    def generateData: Array[DataPoint] = {
      def generatePoint(i:Int): DataPoint = {
        val y = if(i%2 == 0) -1 else 1
        val x = DenseVector.fill(D) { rand.nextGaussian() + y + R }
        DataPoint(x, y)
      }
      Array.tabulate(N)(generatePoint)
    }

    val data = generateData

    // Initialize w to a random value
    var w = DenseVector.fill(D) { 2 * rand.nextDouble - 1 }
    println("Initial w: " + w)

    for(i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      var gradient: DenseVector[Double] = DenseVector.zeros[Double](D)
      for(p <- data) {
        val scale = (1 / (1 + math.exp(-p.y * w.dot(p.x))) - 1) * p.y
        gradient += p.x * scale
      }
      w -= gradient
    }

    println("Final w: " + w)
  }

}
