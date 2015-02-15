package com.github.debop.spark.test

/**
 * SparkTestExample
 * @author sunghyouk.bae@gmail.com 15. 2. 15.
 */
class SparkTestExample extends AbstractSparkFunSuite {

  sparkTest("spark filter") {
    val data = sc.parallelize(1 to 1e6.toInt)
    data.filter(_ % 2 == 0).count shouldEqual 5e5.toInt
  }

  test("non-spark code") {
    val x = 17
    val y = 3

    (x + y) shouldEqual 20
  }

}
