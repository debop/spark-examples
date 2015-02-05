package learningsparkexamples.basic

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * BasicAvgFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class BasicAvgFunSuite extends AbstractSparkFunSuite {

  var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val master = "local"
    sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
  }

  override protected def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }


  test("basic avg") {
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    val avg = result._1 / result._2.toFloat
    println(s"Avg of List(1,2,3,4) = $avg, result=$result")
  }

  test("basic avg from File") {
    val inputFile = "files/basicAvg.txt"

    val input = sc.textFile(inputFile)
    val result = input.map(_.toInt)
                 .aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1), (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val avg = result._1 / result._2.toFloat
    println(s"Avg of List(1,2,3,4) = $avg, result=$result")
  }

  test("basic avg from Directory") {
    val inputFile = "files/basicavgs"
    val outputFile = "files/basicavgs/output"

    deleteDirectory(outputFile)

    val input = sc.wholeTextFiles(inputFile)
    val result = input.mapValues { y =>
      val nums = y.split(" ").map(_.toDouble)
      nums.sum / nums.size.toDouble
    }
    result.saveAsTextFile(outputFile)
  }

  test("map partitions") {
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.mapPartitions(partition => Iterator(AvgCount(0, 0).merge(partition))).reduce((x, y) => x.merge(y))

    println(s"map partition result=$result")
  }


}

case class AvgCount(var total: Int = 0, var num: Int = 0) {
  def merge(other: AvgCount): AvgCount = {
    total += other.total
    num += other.num
    this
  }

  def merge(input: Iterator[Int]): AvgCount = {
    input.foreach { elem =>
      total += elem
      num += 1
    }
    this
  }

  def avg(): Float = total / num.toFloat
}
