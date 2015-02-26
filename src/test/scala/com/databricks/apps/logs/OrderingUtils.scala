package com.databricks.apps.logs

/**
 * OrderingUtils
 * @author sunghyouk.bae@gmail.com at 15. 2. 26.
 */
object OrderingUtils {

  object SecondValueOrdering extends Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      x._2 compare y._2
    }
  }

  object SecondValueLongOrdering extends Ordering[(String, Long)] {
    override def compare(x: (String, Long), y: (String, Long)): Int = {
      x._2 compare y._2
    }
  }

}
