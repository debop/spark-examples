package org.apache.spark.examples.sql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.examples.AbstractSparkExample

case class Record(key: Int, value: String)

class RDDRelation extends AbstractSparkExample {
  test("RDDRelation") {
    val conf = new SparkConf().setMaster("local").setAppName("RDDRelation")
    val sc = new SparkContext(conf)



    sc.stop()
  }
}
