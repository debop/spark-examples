package org.apache.spark.examples.mllib

import org.apache.spark.SparkContext._
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object SampledRDDs {

  case class Params(input: String = "data/mllib/sample_binary_classification_data.txt",
                    fraction: Double = 0.1) extends AbstractParams[Params]

}

class SampledRDDs extends AbstractSparkExample {

  import org.apache.spark.examples.mllib.SampledRDDs._

  sparkTest("Sampled RDD - fraction 0.1") {
    run(Params(fraction = 0.1))
  }

  sparkTest("Sampled RDD - fraction 0.2") {
    run(Params(fraction = 0.2))
  }

  sparkTest("Sampled RDD - fraction 0.5") {
    run(Params(fraction = 0.5))
  }

  def run(params: Params): Unit = {

    val examples = MLUtils.loadLibSVMFile(sc, params.input)
    val numExamples = examples.count()
    if (numExamples == 0) {
      throw new RuntimeException("Error: Data file had no samples to load.")
    }
    println(s"Loaded data with $numExamples examples from file: ${params.input}")

    // Example: RDD.sample() and RDD.takeSample()
    val expectedSampleSize = (numExamples * params.fraction).toInt
    println(s"Sampling RDD using fraction ${params.fraction}. Expected sample size = $expectedSampleSize.")

    val sampledRDD = examples.sample(withReplacement = true, fraction = params.fraction)
    println(s"  RDD.sample(): sample has ${sampledRDD.count()} examples")

    val sampledArray = examples.takeSample(withReplacement = true, num = expectedSampleSize)
    println(s"  RDD.takeSample(): sample has ${sampledArray.size} examples")

    println()

    // Example: RDD.sampleByKey() and RDD.sampleByKeyExact()
    val keyedRDD = examples.map { lp => (lp.label.toInt, lp.features)}
    println(s"  Keyed data using label (Int) as key ==> Orig")
    // Count examples per label in original data.
    val keyCounts = keyedRDD.countByKey()

    // Subsample, and count examples per label in sampled data. (approximate)
    val fractions: Map[Int, Double] = keyCounts.keys.map((_, params.fraction)).toMap
    val sampledByKeyRDD: RDD[(Int, Vector)] = keyedRDD.sampleByKey(withReplacement = true, fractions = fractions)
    val keyCountsB: collection.Map[Int, Long] = sampledByKeyRDD.countByKey()
    val sizeB: Long = keyCountsB.values.sum
    println(s"  Sampled $sizeB examples using approximate stratified sampling (by label). ==> Approx Sample")

    // Subsample, and count examples per label in sampled data. (exact)
    val sampledByKeyRDDExact: RDD[(Int, Vector)] = keyedRDD.sampleByKeyExact(withReplacement = true, fractions = fractions)
    val keyCountsBExact: collection.Map[Int, Long] = sampledByKeyRDDExact.countByKey()
    val sizeBExact: Long = keyCountsBExact.values.sum
    println(s"  Sampled $sizeBExact examples using exact stratified sampling (by label). ==> Exact Sample")

    // Compare samples
    println(s"   \tFractions of examples with key")
    println(s"Key\tOrig\tApprox Sample\tExact Sample")

    keyCounts.keys.toSeq.sorted.foreach { key =>
      val origFrac = keyCounts(key) / numExamples.toDouble
      val approxFrac =
        if (sizeB != 0) keyCountsB.getOrElse(key, 0L) / sizeB.toDouble
        else 0

      val exactFrac =
        if (sizeBExact != 0) keyCountsBExact.getOrElse(key, 0L) / sizeBExact.toDouble
        else 0

      println(s"$key\t$origFrac\t$approxFrac\t$exactFrac")
    }

  }
}
