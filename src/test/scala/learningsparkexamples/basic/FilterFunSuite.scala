package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * FilterFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class FilterFunSuite extends AbstractSparkFunSuite {

  test("filter union combo") {
    val conf = new SparkConf().setMaster("local").setAppName("FilterUnionComob")
    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile("files/log-output.log").persist()
    val errorsRDD = inputRDD.filter(_.contains("ERROR"))
    val warningRDD = inputRDD.filter(_.contains("WARN"))
    val badLinesRDD = errorsRDD.union(warningRDD)

    println("bad lines:\n" + badLinesRDD.collect().mkString("\n"))

    sc.stop()
  }

  test("intersect by key") {
    val conf = new SparkConf().setMaster("local").setAppName("FilterUnionComob")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List((1, "panda"), (2, "happy")))
    val rdd2 = sc.parallelize(List((2, "panda"), (3, "china")))

    val iRdd = intersectByKey(rdd1, rdd2)
    val panda: List[(Int, String)] = iRdd.collect().toList

    panda.map(println)

    sc.stop()
  }

  /**
   * 공통되는 키를 가진 것만 선택합니다. (교집합)
   */
  def intersectByKey[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): RDD[(K, V)] = {
    rdd1.cogroup(rdd2).flatMapValues {
      case (Nil, _) => None
      case (_, Nil) => None
      case (x, y) => x ++ y
    }
  }
}
