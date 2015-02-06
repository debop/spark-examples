package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random
import org.apache.spark.SparkContext._

/**
 * Transitive closure on a graph.
 */
class SparkTC extends AbstractSparkExample {

  test("transitive closure") {
    val conf = new SparkConf().setMaster("local").setAppName("SparkTC")
    val sc = new SparkContext(conf)

    var tc = sc.parallelize(SparkTC.generateGraph).cache()

    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))

    // This join is iterated until a fixed point is reached.
    var oldCount = 0L
    var nextCount = tc.count()

    do {
      oldCount = nextCount

      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to object the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache()
      nextCount = tc.count()
    } while(nextCount != oldCount)

    println(s"TC has ${tc.count()} edges.")
    sc.stop()
  }

}

object SparkTC {

  val numEdges = 200
  val numVertices = 100
  val rand = new Random(42)

  def generateGraph: Seq[(Int, Int)] = {
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
    while (edges.size < numEdges) {
      val from = rand.nextInt(numVertices)
      val to = rand.nextInt(numVertices)
      if (from != to) edges += ((from, to))
    }
    edges.toSeq
  }
}
