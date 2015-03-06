package fastdataprocessing.pandaspark.examples

import java.util.Random

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * GroupByExample
 * @author sunghyouk.bae@gmail.com at 15. 3. 6.
 */
class GroupByExample extends AbstractFastDataProcessing {

  sparkTest("group by test") {
    val numMappers = 2
    val numKVPairs = 1000
    val valSize = 1000
    val numReducers = numMappers

    val pairs1: RDD[(Int, Array[Byte])] = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val rnd = new Random()
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        rnd.nextBytes(byteArr)
        arr1(i) = (rnd.nextInt(numKVPairs), byteArr)
      }
      arr1
    }.cache()

    // 위의 flatMap 작업이 실제로 작업이 되도록 합니다.
    pairs1.count()
    val grouped = pairs1.groupByKey(numReducers).cache()

    println(s"group by pairs count = ${grouped.count()}")
    println(s"group by pairs = ${grouped.collect().mkString("\n")}")
  }

  sparkTest("group by and then sum") {
    val numMappers = 2
    val numKVPairs = 10000

    val pairs = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val rnd = new Random()
      val arr = new Array[(Int, Int)](numKVPairs)
      for (i <- 0 until numKVPairs) {
        arr(i) = (rnd.nextInt(20), rnd.nextInt(100))
      }
      arr
    }.cache()

    pairs.cache()

    val pandas: RDD[(Int, Int)] = pairs.groupByKey().map(x => (x._1, x._2.sum))
    val otherPandas: RDD[(Int, Int)] = pairs.reduceByKey((x, y) => x + y)
    val foldExample: RDD[(Int, Int)] = pairs.groupByKey().mapValues(x => x.sum)
    val coGroupedPandas: RDD[(Int, (Iterable[Int], Iterable[Int]))] = pandas.cogroup(otherPandas)

    pandas.cache()
    otherPandas.cache()
    coGroupedPandas.cache()

    println(s"Group by then sum: ${pandas.collect().toList}")
    println(s"Reduce by key: ${otherPandas.collect().toList}")
    println(s"co grouped: ${coGroupedPandas.collect().toList}")
  }

}
