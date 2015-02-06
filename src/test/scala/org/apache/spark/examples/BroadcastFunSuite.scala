package org.apache.spark.examples

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class BroadcastFunSuite extends AbstractSparkExample {

  test("broadcast") {
    val conf = new SparkConf().setMaster("local").setAppName("Broadcast Test")
    .set("spark.broadcast.factory", s"org.apache.spark.broadcast.HttpBroadcastFactory")
    .set("spark.broadcast.blockSize", "4096")

    val sc = new SparkContext(conf)

    val slices = 2
    val num = 1000000
    val arr1 = (0 until num).toArray

    for(i <- 0 until 3) {
      println("Iteration " + i)
      println("==================")
      val startTime = System.nanoTime()
      val barr1: Broadcast[Array[Int]] = sc.broadcast(arr1)
      val observedSizes: RDD[Int] = sc.parallelize(1 to 10, slices).map(_ => barr1.value.size)
      observedSizes.collect().foreach(println)
      println(s"Iteration $i took ${(System.nanoTime()-startTime)/1E6}")
    }

    sc.stop()
  }

}
