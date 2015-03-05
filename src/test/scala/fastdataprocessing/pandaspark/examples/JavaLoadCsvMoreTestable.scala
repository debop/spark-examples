package fastdataprocessing.pandaspark.examples

import java.io.StringReader
import java.lang.Iterable
import java.util

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.Accumulator
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control.NonFatal
import org.apache.spark.SparkContext._


object JavaLoadCsvMoreTestable {

  private def parseLine(acc: Accumulator[Int], line: String): Seq[Array[Int]] = {
    val result = mutable.ArrayBuffer[Array[Int]]()
    try {
      val reader = new CSVReader(new StringReader(line))
      val parsedLine = reader.readNext()
      val intLine = parsedLine.map(x => x.toInt)
      result += intLine
    } catch {
      case NonFatal(e) =>
        acc.add(1)
    }
    result
  }

  def processData(acc: Accumulator[Int], input: RDD[String]): RDD[Double] = {
    val splitLines = input.flatMap(line => parseLine(acc, line))
    splitLines.map { arr => arr.sum.toDouble}
  }
}

class JavaLoadCsvMoreTestable extends AbstractFastDataProcessing {

  sparkTest("java load csv more testable") {

    val inFile = sc.textFile(EXAMPLE_CSV).cache()
    val summedData: RDD[Double] = JavaLoadCsvMoreTestable.processData(sc.accumulator(0, "invalid count"), inFile)
    println(s"Summed Data = ${summedData.collect().mkString(",")}")
  }

}

