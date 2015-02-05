package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.rdd.RDD

class HdfsFunSuite extends AbstractSparkExample {

  test("HDFS File") {
    val conf = new SparkConf().setMaster("local").setAppName("Hdfs Test")
    val sc = new SparkContext(conf)

    val file = sc.textFile("files/spam.txt")
    val mapped: RDD[Int] = file.map(s => s.length).cache()

    (1 to 10) foreach { i =>
      val start = System.currentTimeMillis()
      for(x <- mapped) { x + 2 }
      println(s"mapped=${mapped.collect().mkString(",")}")
      val end = System.currentTimeMillis()

      println(s"Iteration $i took ${end-start} ms")
    }
    sc.stop()
  }

}
