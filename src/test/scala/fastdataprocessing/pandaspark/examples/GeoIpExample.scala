package fastdataprocessing.pandaspark.examples

import au.com.bytecode.opencsv.CSVReader
import breeze.io.TextReader.StringReader
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.linalg.Vector

import scala.util.control.NonFatal

case class DataPoint(x: Vector, y: Double)
/**
 * GeoIpExample
 * @author sunghyouk.bae@gmail.com at 15. 3. 5.
 */
class GeoIpExample extends AbstractFastDataProcessing {

  sparkTest("Geo IP Example") {

    //    val inputFile = "data/example_partially_invalid.csv"
    //    val maxMindPath = "data/GeoLiteCity.dat"
    //
    //    val invalidLineCounter = sc.accumulator(0)
    //    val inFile = sc.textFile(inputFile)
    //    val parsedInput = inFile.flatMap { line =>
    //      try {
    //        val row = new CSVReader(new StringReader(line)).readNext()
    //        Some((row(0), row.drop(1).map(_.toDouble)))
    //      } catch {
    //        case NonFatal(e) =>
    //          invalidLineCounter += 1
    //          None
    //      }
    //    }
    //
    //    val geoFile = sc.addFile(maxMindPath)
    //
    //    // getLocation gives back an option so we use flatMap to only output if its a some type
    //    val ipContries = parsedInput.flatMap()
  }

}
