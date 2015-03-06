package fastdataprocessing.pandaspark.examples

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD

/**
 * LoadCsvExample
 * @author sunghyouk.bae@gmail.com at 15. 3. 6.
 */
class LoadCsvExample extends AbstractFastDataProcessing {

  sparkTest("load csv example") {
    val inputFile = NUMBERS_CSV

    sc.addFile(inputFile)
    val csvRDD: RDD[String] = sc.textFile(inputFile)

    val lines = csvRDD.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }.cache()

    val numericData: RDD[Array[Double]] = lines.map(line => line.map(_.toDouble)).cache()
    val summedData = numericData.map(xs => xs.sum)

    println(summedData.collect().mkString(","))
  }

}
