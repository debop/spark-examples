package org.apache.spark.examples.ml

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row

/**
 * A simple example demonstrating ways to specify parameters for Estimators and Transformers.
 */
class SimpleParamsExample extends AbstractSparkExample {

  sparkTest("simple params example") {

    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val training: RDD[LabeledPoint] = sc.parallelize(Seq(
                                                          LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                                                          LabeledPoint(1.0, Vectors.dense(2.0, 1.0, -1.0)),
                                                          LabeledPoint(1.0, Vectors.dense(2.0, 1.3, 1.0)),
                                                          LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))
                                                        ))

    // Create a LogisticRegression instance. This instance is an Estimator.
    val lr = new LogisticRegression()
    println(s"LogisticRegression parameters:\n${lr.explainParams()}\n")

    lr.setMaxIter(10).setRegParam(0.01)

    // Learn a LogisticRegression model. This uses the parameters stored in lr.
    val model1 = lr.fit(training)

    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    println("Model 1 was fit using parameters: " + model1.fittingParamMap)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
    paramMap.put(lr.maxIter, 30) // Specify 1 Param. This overwrites the original maxIter.
    paramMap.put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.scoreCol -> "probability")
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.fittingParamMap)

    val test: RDD[LabeledPoint] = sc.parallelize(Seq(
                                                      LabeledPoint(-1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                                                      LabeledPoint(3.0, Vectors.dense(3.0, 2.0, -1.0)),
                                                      LabeledPoint(0.0, Vectors.dense(0.0, 2.2, -1.5))
                                                    ))

    // Make predictions on test documents using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'probability' column instead of the usual 'score'
    // column since we renamed the lr.scoreCol parameter previously.
    model2.transform(test)
    .select('features, 'label, 'probability, 'prediction)
    .collect()
    .foreach {
      case Row(features: Vector, label: Double, prob: Double, prediction: Double) =>
        println(s"$label, $features) -> prob=$prob, prediction=$prediction")
    }

    model2.transform(test).collect().foreach { x =>
      println(x)

    }
  }
}
