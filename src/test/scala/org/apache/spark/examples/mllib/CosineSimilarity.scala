package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Compute the similar columns of a matrix, using cosine similarity.
 *
 * The input matrix must be stored in row-oriented dense format, one line per row with its entries
 * separated by space. For example,
 * {{{
 * 0.5 1.0
 * 2.0 3.0
 * 4.0 5.0
 * }}}
 * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
 *
 * Example invocation:
 *
 * bin/run-example mllib.CosineSimilarity \
 * --threshold 0.1 data/mllib/sample_svm_data.txt
 */
class CosineSimilarity extends AbstractSparkExample {

  case class Params(input: String = "data/mllib/sample_svm_data.txt",
                    threshold: Double = 0.1) extends AbstractParams[Params]

  sparkTest("Cosine Similarity - threshold = 0.05") {
    run(Params(threshold = 0.05))
  }

  sparkTest("Cosine Similarity - threshold = 0.1") {
    run(Params())
  }

  sparkTest("Cosine Similarity - threshold = 0.3") {
    run(Params(threshold = 0.3))
  }

  def run(params: Params): Unit = {

    // Load and parse the data file.
    val rows: RDD[Vector] = sc.textFile(params.input).map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }.cache()

    val mat = new RowMatrix(rows)

    // Compute similar columns perfectly, with brute force.
    val exact: CoordinateMatrix = mat.columnSimilarities()

    // Compute similar columns with estimation using DIMSUM
    val approx: CoordinateMatrix = mat.columnSimilarities(params.threshold)

    val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => ((i, j), u)}
    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v)}

    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
      case (u, Some(v)) => math.abs(u - v)
      case (u, None) => math.abs(u)
    }.mean()

    println(s"Average absolute error in estimate is : $MAE, threshold=${params.threshold}")
  }


}
