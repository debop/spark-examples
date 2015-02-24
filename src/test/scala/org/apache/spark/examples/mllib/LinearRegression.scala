package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.optimization.{L1Updater, SimpleUpdater, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

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
                     input: String = "data/mllib/sample_linear_regression_data.txt",
                     numIterations: Int = 1000,
                     stepSize: Double = 1.0,
                     regType: RegType = L2,
                     regParam: Double = 0.01
                     ) extends AbstractParams[Params]
}

class LinearRegression extends AbstractSparkExample {

  import org.apache.spark.examples.mllib.LinearRegression.RegType._
  import org.apache.spark.examples.mllib.LinearRegression._

  sparkTest("linear regression - NONE") {
    run(Params(regType = NONE))
  }
  sparkTest("linear regression - L1") {
    run(Params(regType = L1))
  }
  sparkTest("linear regression - L2") {
    run(Params(regType = L2))
  }

  def run(params: Params) {

    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()

    val splits: Array[RDD[LabeledPoint]] = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()

    println(s"Training: $numTraining, test: $numTest")

    examples.unpersist(blocking = false)

    val updater = params.regType match {
      case NONE => new SimpleUpdater()
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
    .setNumIterations(params.numIterations)
    .setStepSize(params.stepSize)
    .setUpdater(updater)
    .setRegParam(params.regParam)

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
}
