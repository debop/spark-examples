package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


class SparkPageRank extends AbstractSparkExample {

  sparkTest("page rank") {

    val input = "data/mllib/pagerank_data.txt"
    val iters = 10

    val lines: RDD[String] = sc.textFile(input, 1)
    val links: RDD[(String, Iterable[String])] = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    var ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)
    println("initial ranks: " + ranks.collect().mkString("\n"))

    for (i <- 1 to iters) {
      val contribs: RDD[(String, Double)] = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output: Array[(String, Double)] = ranks.collect()
    output.foreach(tup => println("Page " + tup._1 + " has ranks: " + tup._2))
  }

}
