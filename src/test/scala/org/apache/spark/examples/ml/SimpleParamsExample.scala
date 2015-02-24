package org.apache.spark.examples.ml

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._

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
  }
}
