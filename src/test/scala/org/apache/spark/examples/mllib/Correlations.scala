package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD


/**
 * An example app for summarizing multivariate data from a file. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.Correlations
 * }}}
 * By default, this loads a synthetic dataset from `data/mllib/sample_linear_regression_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object Correlations {

  case class Params(input: String = "data/mllib/sample_linear_regression_data.txt",
                    method: String = "pearson") extends AbstractParams[Params]

  val defaultParams = Params()

}

class Correlations extends AbstractSparkExample {

  import org.apache.spark.examples.mllib.Correlations._

  sparkTest("Correlation test with pearson") {
    run(Params())
  }

  sparkTest("Correlation test with spearman") {
    run(Params(method = "spearman"))
  }

  def run(params: Params): Unit = {

    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, params.input).cache()

    println(s"Summary of data file: ${params.input}")
    println(s"${examples.count()} data points")

    val labelRDD: RDD[Double] = examples.map(_.label)
    val numFeatures = examples.take(1)(0).features.size
    val corrType = params.method

    println()
    println(s"Correlation ($corrType) between label and each feature")
    println(s"Feature\tCorrelation")

    var feature = 0
    while (feature < numFeatures) {
      val featureRDD = examples.map(_.features(feature))
      val corr = Statistics.corr(labelRDD, featureRDD, corrType)
      println(s"$feature\t$corr")
      feature += 1
    }
    println()
  }
}
