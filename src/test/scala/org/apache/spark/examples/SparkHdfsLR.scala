package org.apache.spark.examples

import java.util.Random

import breeze.linalg._
import breeze.numerics.exp
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.InputFormatInfo

class SparkHdfsLR extends AbstractSparkExample {

  import org.apache.spark.examples.SparkHdfsLR._

  test("Spark Logistic regression based classification") {

    val inputPath = "data/mllib/lr_data.txt"

    val hdConf = new Configuration()
    val conf = new SparkConf().setMaster("local").setAppName("SparkHdfsLR")
    val sc = new SparkContext(conf,
                               InputFormatInfo.computePreferredLocations(Seq(new InputFormatInfo(hdConf,
                                                                                                  classOf[org.apache.hadoop.mapred.TextInputFormat],
                                                                                                  inputPath))))

    val lines: RDD[String] = sc.textFile(inputPath)
    val points: RDD[DataPoint] = lines.map(parsePoint).cache()
    val ITERATIONS = 10

    // Initialize w to a random value
    var w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println(s"Initialize w: " + w)

    for (i <- 1 to ITERATIONS) {
      println(s"On iteration $i")
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
object SparkHdfsLR {

  val D = 10
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, " ")
    val y = tok.nextToken.toDouble
    val x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble
      i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

}
