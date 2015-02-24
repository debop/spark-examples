package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Train a linear regression model on one stream of data and make predictions
 * on another stream, where the data streams arrive as text files
 * into two different directories.
 *
 * The rows of the text files must be labeled data points in the form
 * `(y,[x1,x2,x3,...,xn])`
 * Where n is the number of features. n must be the same for train and test.
 *
 * Usage: StreamingLinearRegression <trainingDir> <testDir> <batchDuration> <numFeatures>
 *
 * To run on your local machine using the two directories `trainingDir` and `testDir`,
 * with updates every 5 seconds, and 2 features per data point, call:
 * $ bin/run-example \
 * org.apache.spark.examples.mllib.StreamingLinearRegression trainingDir testDir 5 2
 *
 * As you add text files to `trainingDir` the model will continuously update.
 * Anytime you add text files to `testDir`, you'll see predictions from the current model.
 *
 */
class StreamingLinearRegression extends AbstractSparkExample {

  test("Streaming Linear Regression") {

    val trainingFile = "data/mllib/sample_libsvm_data.txt"
    val testFile = "data/mllib/sample_libsvm_data.txt"

    val conf = newSparkConf("Streaming KMeans Example")
    val ssc = new StreamingContext(conf, Seconds(2L))

    val trainingData = ssc.textFileStream(trainingFile).map(LabeledPoint.parse)
    val testData = ssc.textFileStream(testFile).map(LabeledPoint.parse)

    val model = new StreamingLinearRegressionWithSGD()
                .setInitialWeights(Vectors.zeros(3))

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination(10000)

  }

}
