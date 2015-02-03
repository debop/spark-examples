package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * MapFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class MapFunSuite extends AbstractSparkFunSuite {

  test("map") {
    val sc = new SparkContext("local", "BasicMap", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(s"square : ${result.collect().mkString(",")}")

    sc.stop()
  }

  test("map no cache") {
    val sc = new SparkContext("local", "BasicMapNoCache", System.getenv("SPARK_HOME"))

    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)

    // will compute result twice
    println(result.count())
    println(s"square : ${result.collect().mkString(",")}")

    sc.stop()
  }

  test("map partitions") {
    val sc = new SparkContext("local", "BasicMapPartitions", System.getenv("SPARK_HOME"))

    val input = sc.parallelize(List("KK6JKQ", "Ve3UoW", "kk5jlk", "W6BB"))

    val result: RDD[String] = input.mapPartitions { signs =>
      val client = new HttpClient()
      signs.map { sign =>
        val method = new GetMethod(s"http://qrzcq.com/call/$sign")
        client.executeMethod(method)
        method
      }.map { method =>
        method.getResponseBodyAsString
      }
    }

    println("result= " + result.collect().mkString(","))

    sc.stop()
  }

  test("map then filter") {
    val sc = new SparkContext("local", "BasicMapThenFilter", System.getenv("SPARK_HOME"))

    val input = sc.parallelize(List(1, 2, 3, 4))
    val squared = input.map(x => x * x)
    val result = squared.filter(x => x != 1)

    result.collect().toList shouldEqual List(4, 9, 16)

    sc.stop()
  }

}
