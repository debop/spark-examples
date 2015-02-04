package learningsparkexamples.basic

import java.io.File
import java.sql.{DriverManager, ResultSet}

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.SQLContext

import scala.slick.direct.AnnotationMapper.column
import scala.slick.lifted.Tag
import scala.slick.model.Table
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

  test("load string int file") {
    val sc = new SparkContext("local", "Load Sequence File", System.getenv("SPARK_HOME"))

    val output = "files/load/output"
    FileUtils.deleteDirectory(new File(output))

    val input = sc.parallelize(List(("panda", 2), ("happy", 3), ("china", 1)))
    input.saveAsSequenceFile(output)

    val data = sc.sequenceFile("files/load/output/part-00000", classOf[Text], classOf[IntWritable])
               .map { case (x, y) => (x.toString, y.get())}

    println(s"data=${data.collect().toList}")

    sc.stop()
  }

  test("Load json with Spark SQL") {
    val sc = new SparkContext("local", "Load Json with Spark SQL", System.getenv("SPARK_HOME"))
    val sqlCtx = new SQLContext(sc)

    val inputFile = "files/pandainfo.json"
    val input = sqlCtx.jsonFile(inputFile)

    input.printSchema()

    sc.stop()
  }

  test("load key value from Text") {
    val sc = new SparkContext("local", "Load Key Value from Text", System.getenv("SPARK_HOME"))
    val inputFile = "files/key_value.txt"
    val input: RDD[(String, String)] =
      sc
      .hadoopFile[Text, Text, KeyValueTextInputFormat](inputFile)
      .map {
        case (x, y) => (x.toString, y.toString)
      }
    println(s"key value: ${input.collect().toList}")
    sc.stop()
  }


}
