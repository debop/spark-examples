package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext._
import org.apache.spark._

import scala.util.control.NonFatal

/**
 * LoadFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class LoadFunSuite extends AbstractSparkFunSuite {

  test("load nums") {
    val sc = new SparkContext("local", "LoadNums", System.getenv("SPARK_HOME"))

    val inputFile = "files/string-int.txt"

    // val file = sc.textFile(inputFile)

    val file = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 3), ("happy", 2), ("china", 1)))
    val errorLines = sc.accumulator(0) // create an Accumulator[Int] initialized to 0
    val dataLines = sc.accumulator(0) // create a second Accumulator[Int] initialized

    //    val words = file.flatMap(line => line.split(" "))
    //    println(s"words=$words")
    //    words.collect().foreach(println)


    val counts = file.flatMap { line =>
      println(s"line=$line")

      try {
        // val input = line.split(" ")
        val data = Some(line._1, line._2)
        dataLines += 1
        data
      } catch {
        case e: NumberFormatException =>
          errorLines += 1
          None
        case e: ArrayIndexOutOfBoundsException =>
          errorLines += 1
          None
        case NonFatal(e) =>
          println(s"Error for parsing", e)
          errorLines += 1
          None
      }
    }.reduceByKey(_ + _)

    if (errorLines.value < 0.1 * dataLines.value) {
      counts.saveAsTextFile("output.txt")
    } else {
      println(s"Too many errors ${errorLines.value} for ${dataLines.value}")
    }

    sc.stop()
  }

}
