package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.{SparkConf, SparkContext}

/**
 * BasicAvgWithKyroFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class BasicAvgWithKyroFunSuite extends AbstractSparkFunSuite {

  test("with Kryo") {
    val conf = new SparkConf().setMaster("local").setAppName("basicAvgWithKyro")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    val avg = result._1 / result._2.toFloat
    println(s"Avg of List(1,2,3,4) = $avg, result=$result")

    sc.stop()
  }
}
