package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.optimization.{L1Updater, SimpleUpdater, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{LoggerFactory, Logger}

/**
 * LinearRegression
 * @author sunghyouk.bae@gmail.com
 */
object LinearRegression {
  object RegType extends Enumeration {
    type RegType = Value
    val NONE, L1, L2 = Value
  }

  import org.apache.spark.examples.mllib.LinearRegression.RegType._

  case class Params(
                     input: String = null,
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     regType: RegType = L2,
                     regParam: Double = 0.01
                     ) extends AbstractParams[Params]

  val defaultParams = Params()

}

class LinearRegression extends AbstractSparkExample {

  import org.apache.spark.examples.mllib.LinearRegression.RegType._
  import org.apache.spark.examples.mllib.LinearRegression._

  test("linear regression") {
    val conf = new SparkConf().setMaster("local").setAppName("LinearRegression")
    val sc = new SparkContext(conf)

    val examples = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_linear_regression_data.txt").cache()

    val splits: Array[RDD[LabeledPoint]] = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()

    println(s"Training: $numTraining, test: $numTest")

    examples.unpersist(blocking = false)

    RegType.values.foreach { regType =>

      val updater = regType match {
        case NONE => new SimpleUpdater()
        case L1 => new L1Updater()
        case L2 => new SquaredL2Updater()
      }

      val algorithm = new LinearRegressionWithSGD()
      algorithm.optimizer
      .setNumIterations(defaultParams.numIterations)
      .setStepSize(defaultParams.stepSize)
      .setUpdater(updater)
      .setRegParam(defaultParams.regParam)

      val model: LinearRegressionModel = algorithm.run(training)

      val prediction = model.predict(test.map(_.features))
      val predictionAndLabel = prediction.zip(test.map(_.label))

      val loss = predictionAndLabel.map { case (p, l) =>
        val err = p - l
        err * err
      }.reduce(_ + _)

      val rmse = math.sqrt(loss / numTest)

      println(s"Test RMSE [${updater.getClass.getName}]- $rmse")
    }

    sc.stop()
  }
}
