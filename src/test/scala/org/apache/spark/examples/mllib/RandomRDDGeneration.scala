package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.random.RandomRDDs

/**
 * RandomRDDGeneration
 * @author sunghyouk.bae@gmail.com at 15. 2. 24.
 */
class RandomRDDGeneration extends AbstractSparkExample {

  sparkTest("random RDD generation") {
    val numExamples = 10000 // number of examples to generate
    val fraction = 0.1 // fraction of data to sample

    // Example: RandomRDDs.normalRDD
    val normalRDD = RandomRDDs.normalRDD(sc, numExamples)
    println(s"Generated RDD of ${normalRDD.count()}" +
            s" examples sampled from the standard normal distribution")
    println("  First 5 samples:")
    normalRDD.take(5).foreach(x => println(s"\t$x"))

    // Example: RandomRDDs.normalVectorRDD
    val normalVectorRDD = RandomRDDs.normalVectorRDD(sc, numRows = numExamples, numCols = 3)
    println(s"Generated RDD of ${normalVectorRDD.count()} examples of length-2 vectors.")
    println("  First 5 samples:")
    normalVectorRDD.take(5).foreach(x => println(s"\t$x"))
  }

}
