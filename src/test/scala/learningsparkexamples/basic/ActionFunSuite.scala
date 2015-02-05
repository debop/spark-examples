package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * ActionFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class ActionFunSuite extends AbstractSparkFunSuite {

  test("sum") {
    val sc = new SparkContext("local", "Sum", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(0 until 10000)
    val result = input.fold(0)((x, y) => x + y)
    println(s"sum=$result")
    sc.stop()
  }

  test("per key avg") {
    val sc = new SparkContext("local", "PerKeyAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 4)))

    val result: RDD[(String, Float)] =
      input.combineByKey(
                          (v) => (v, 1),
                          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
                          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
                        )
      .map {
        case (key, value) => (key, value._1 / value._2.toFloat)
      }

    result.collectAsMap().map(println)
    sc.stop()
  }

  test("remove outliers") {
    val sc = new SparkContext("local", "RemoveOutliers", System.getenv("SPARK_HOME"))
    val input: RDD[Double] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1000)).map(_.toDouble)
    val result: RDD[Double] = removeOutliers(input)
    println("remove outliers: " + result.collect().mkString(","))
  }

  def removeOutliers(rdd: RDD[Double]): RDD[Double] = {
    val summaryStats = rdd.stats()
    val stddev = math.sqrt(summaryStats.variance)
    rdd.filter(x => math.abs(x - summaryStats.mean) < 3 * stddev)
  }
}
