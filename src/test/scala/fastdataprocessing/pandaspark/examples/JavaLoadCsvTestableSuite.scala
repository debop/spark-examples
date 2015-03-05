package fastdataprocessing.pandaspark.examples

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.SparkContext._

/**
 * JavaLoadCsvTestableSuite
 * @author sunghyouk.bae@gmail.com at 15. 3. 5.
 */
class JavaLoadCsvTestableSuite extends AbstractFastDataProcessing {

  sparkTest("java load csv") {
    val counter = sc.accumulator(0)
    val input = sc.parallelize(List("1,2", "1,3", "murh"))

  }

}
