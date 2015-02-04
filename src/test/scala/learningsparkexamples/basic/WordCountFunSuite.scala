package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
 * WordCountFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class WordCountFunSuite extends AbstractSparkFunSuite {

  test("word count") {
    val sc = new SparkContext("local", "WordCount", System.getenv("SPARK_HOME"))
    val input: RDD[String] = sc.parallelize(List("pandas", "i like pandas"))
    val words: RDD[String] = input.flatMap(line => line.split(" "))
    val wc: Map[String, Long] = words.countByValue()
    println(s"word count = ${wc.mkString(",")}")
    sc.stop()
  }

}
