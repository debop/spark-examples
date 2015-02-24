package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * DenseKMeans
 * @author sunghyouk.bae@gmail.com at 15. 2. 24.
 */
object DenseKMeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import org.apache.spark.examples.mllib.DenseKMeans.InitializationMode._

  case class Params(
                     input: String = "data/mllib/kmeans_data.txt",
                     k: Int = 2,
                     numIterations: Int = 10,
                     initializationMode: InitializationMode = Parallel
                     ) extends AbstractParams[Params]
}

class DenseKMeans extends AbstractSparkExample {

  import org.apache.spark.examples.mllib.DenseKMeans.InitializationMode._
  import org.apache.spark.examples.mllib.DenseKMeans._

  sparkTest("dense kmeans - parallel") {
    run(Params())
  }

  sparkTest("dense kmeans - Random") {
    run(Params(initializationMode = Random))
  }

  def run(params: Params): Unit = {

    val examples: RDD[Vector] = sc.textFile(params.input)
                                .map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()

    val numExamples = examples.count()
    println(s"example number: $numExamples")

    val initMode: String = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val model: KMeansModel = new KMeans()
                             .setInitializationMode(initMode)
                             .setK(params.k)
                             .setMaxIterations(params.numIterations)
                             .run(examples)

    val cost: Double = model.computeCost(examples)

    println(s"Total cost = $cost")
  }
}
