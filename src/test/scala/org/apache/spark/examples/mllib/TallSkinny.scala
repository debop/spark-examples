package org.apache.spark.examples.mllib

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


class TallSkinny extends AbstractSparkExample {

  /**
   * Compute the principal components of a tall-and-skinny matrix, whose rows are observations.
   *
   * The input matrix must be stored in row-oriented dense format, one line per row with its entries
   * separated by space. For example,
   * {{{
   * 0.5 1.0
   * 2.0 3.0
   * 4.0 5.0
   * }}}
   * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
   */
  sparkTest("Tall Skinny PCA") {
    val input = "data/mllib/tall_skinny.txt"

    val rows: RDD[Vector] = sc.textFile(input).map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }
    val mat = new RowMatrix(rows)

    // Compute principal components.
    val pc = mat.computePrincipalComponents(mat.numCols().toInt)

    println(s"Principal components are:\n$pc")
  }

  /**
   * Compute the singular value decomposition (SVD) of a tall-and-skinny matrix.
   *
   * The input matrix must be stored in row-oriented dense format, one line per row with its entries
   * separated by space. For example,
   * {{{
   * 0.5 1.0
   * 2.0 3.0
   * 4.0 5.0
   * }}}
   * represents a 3-by-2 matrix, whose first row is (0.5, 1.0).
   */
  sparkTest("Tall Skinny SVD") {
    val input = "data/mllib/tall_skinny.txt"

    val rows: RDD[Vector] = sc.textFile(input).map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }
    val mat = new RowMatrix(rows)

    // Compute SVD.
    val pc = mat.computeSVD(mat.numCols().toInt)

    println(s"Singular values are:\n$pc")
  }

}
