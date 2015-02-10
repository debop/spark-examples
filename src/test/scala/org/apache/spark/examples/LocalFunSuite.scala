package org.apache.spark.examples


import breeze.linalg._
import org.apache.commons.math3.linear._

import scala.util.Random

class LocalFunSuite extends AbstractSparkExample {

  /**
   * Logistic regression based classification.
   *
   * This is an example implementation for learning how to use Spark. For more conventional use,
   * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
   * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
   */
  test("Logistic regression based classification") {
    val N = 10000 // number of data points
    val D = 10 // number of dimension
    val R = 0.7 // Scaling factor
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

    val data = generateData

    // Initialize w to a random value
    var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      var gradient: DenseVector[Double] = DenseVector.zeros[Double](D)
      for (p <- data) {
        val scale = (1 / (1 + math.exp(-p.y * w.dot(p.x))) - 1) * p.y
        gradient += p.x * scale
      }
      w -= gradient
    }

    println("Final w: " + w)
  }

  /**
   * Logistic regression based classification.
   *
   * This is an example implementation for learning how to use Spark. For more conventional use,
   * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
   * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
   */
  test("Logistic regression based classification from File") {
    val D = 10 // Numer of dimensions
    val rand = new Random(42)

    case class DataPoint(x: Vector[Double], y: Double)

    def parsePoint(line: String): DataPoint = {
      val nums = line.split(' ').map(_.toDouble)
      DataPoint(new DenseVector(nums.slice(1, D + 1)), nums(0))
    }

    val lines = scala.io.Source.fromFile("data/mllib/lr_data.txt").getLines().toArray
    val points = lines.map(parsePoint)
    val ITERATIONS = 5

    // Initialize w to a random value
    var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      var gradient = DenseVector.zeros[Double](D)
      for (p <- points) {
        val scale = (1 / (1 + math.exp(-p.y * w.dot(p.x))) - 1) * p.y
        gradient += p.x * scale
      }
      w -= gradient
    }

    println("Final w: " + w)
  }

  /**
   * Alternating least squares matrix factorization.
   *
   * This is an example implementation for learning how to use Spark. For more conventional use,
   * please refer to org.apache.spark.mllib.recommendation.ALS
   */
  test("Alternating least squares matrix factorization") {
    // Parameters set through command line arguments
    var M = 0 // Number of movies
    var U = 0 // Number of users
    var F = 0 // Number of features
    var ITERATIONS = 0
    val LAMBDA = 0.01 // Regularization coefficient

    def generateR(): RealMatrix = {
      val mh = randomMatrix(M, F)
      val uh = randomMatrix(U, F)
      mh.multiply(uh.transpose())
    }

    def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
      val r = new Array2DRowRealMatrix(M, U)
      for (i <- 0 until M; j <- 0 until U) {
        r.setEntry(i, j, ms(i).dotProduct(us(j)))
      }
      val diffs = r.subtract(targetR)
      var sumSqs = 0.0
      for (i <- 0 until M; j <- 0 until U) {
        val diff = diffs.getEntry(i, j)
        sumSqs += diff * diff
      }
      math.sqrt(sumSqs / (M.toDouble * U.toDouble))
    }

    def updateMovie(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix): RealVector = {
      var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
      var Xty: RealVector = new ArrayRealVector(F)

      // For each user that rated the movie
      for (j <- 0 until U) {
        val u = us(j)
        // Add u * u^t to XtX
        XtX = XtX.add(u.outerProduct(u))
        // Add u * rating to Xty
        Xty = Xty.add(u.mapMultiply(R.getEntry(i, j)))
      }
      // Add regularization coefficients to diagonal terms
      for (d <- 0 until F) {
        XtX.addToEntry(d, d, LAMBDA * U)
      }

      // Solve it with Cholesky
      new CholeskyDecomposition(XtX).getSolver.solve(Xty)
    }

    def updateUser(j: Int, u: RealVector, ms: Array[RealVector], R: RealMatrix): RealVector = {
      var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
      var Xty: RealVector = new ArrayRealVector(F)

      // For each movie that the user rated
      for (i <- 0 until M) {
        val m = ms(i)
        // Add m * m^t to XtX
        XtX = XtX.add(m.outerProduct(m))
        // Add m * rating to Xty
        Xty = Xty.add(m.mapMultiply(R.getEntry(i, j)))
      }
      // Add regularization coeefficients to diagonal terms
      for (d <- 0 until F) {
        XtX.addToEntry(d, d, LAMBDA * M)
      }
      // Solve it with Cholesky
      new CholeskyDecomposition(XtX).getSolver.solve(Xty)
    }

    def randomVector(n: Int): RealVector =
      new ArrayRealVector(Array.fill(n)(math.random))

    def randomMatrix(rows: Int, cols: Int): RealMatrix =
      new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))


    M = 100
    U = 500
    F = 15

    println(s"Running with M=$M, U=$U, F=$F, iters=$ITERATIONS\n")

    val R = generateR()

    // Initialize m and u randomly
    var ms = Array.fill(M)(randomVector(F))
    var us = Array.fill(U)(randomVector(F))

    // Iteratively update movies then users
    var prevRmse = 100.0
    var diffRmse = 100.0
    var iter = 1
    while (diffRmse >= 1E-5) {
      println(s"Iteration $iter:")
      ms = (0 until M).map(i => updateMovie(i, ms(i), us, R)).toArray
      us = (0 until U).map(j => updateUser(j, us(j), ms, R)).toArray
      val newRmse = rmse(R, ms, us)
      println(s"RMSE = $newRmse")
      println()
      diffRmse = math.abs(newRmse - prevRmse)
      prevRmse = newRmse
      iter += 1
    }
  }


}
