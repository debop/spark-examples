package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD


object BinaryClassification {

  object Algorithm extends Enumeration {
    type Algorithm = Value
    val SVM, LR = Value
  }

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }

  import org.apache.spark.examples.mllib.BinaryClassification.Algorithm._
  import org.apache.spark.examples.mllib.BinaryClassification.RegType._

  case class Params(input: String = "data/mllib/sample_binary_classification_data.txt",
                    numIterations: Int = 100,
                    stepSize: Double = 1.0,
                    algorithm: Algorithm = LR,
                    regType: RegType = L2,
                    regParam: Double = 0.01) extends AbstractParams[Params]

  lazy val defaultParams = Params()
}

/**
 * An example app for binary classification. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.BinaryClassification
 * }}}
 * A synthetic dataset is located at `data/mllib/sample_binary_classification_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
class BinaryClassification extends AbstractSparkExample {

  import org.apache.spark.examples.mllib.BinaryClassification.Algorithm._
  import org.apache.spark.examples.mllib.BinaryClassification.RegType._
  import org.apache.spark.examples.mllib.BinaryClassification._

  sparkTest("Binary classification - LR / L1") {
    run(Params(algorithm = LR, regType = L1))
  }

  sparkTest("Binary classification - LR / L2") {
    run(Params(algorithm = LR, regType = L2))
  }

  sparkTest("Binary classification - SVM / L1") {
    run(Params(algorithm = SVM, regType = L1))
  }

  sparkTest("Binary classification - SVM / L2") {
    run(Params(algorithm = SVM, regType = L2))
  }

  def run(params: Params): Unit = {
    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, defaultParams.input)

    val splits: Array[RDD[LabeledPoint]] = examples.randomSplit(Array(0.8, 0.2))
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest")

    examples.unpersist(false)

    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val model = defaultParams.algorithm match {
      case LR =>
        val algorithm = new LogisticRegressionWithLBFGS()
        algorithm.optimizer
        .setNumIterations(params.numIterations)
        .setUpdater(updater)
        .setRegParam(params.regParam)

        algorithm.run(training).clearThreshold()

      case SVM =>
        val algorithm = new SVMWithSGD()
        algorithm.optimizer
        .setNumIterations(params.numIterations)
        .setStepSize(params.stepSize)
        .setUpdater(updater)
        .setRegParam(params.regParam)

        algorithm.run(training).clearThreshold()
    }

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}")
  }
}
