package fastdataprocessing.pandaspark.examples

import org.apache.spark.examples.AbstractSparkExample

/**
 * AbstractFastDataProcessing
 * @author sunghyouk.bae@gmail.com at 15. 3. 5.
 */
abstract class AbstractFastDataProcessing extends AbstractSparkExample {

  val ROOT_DIR = "data/fastdataprocessing/"
  val EXAMPLE_CSV = ROOT_DIR + "example_partially_invalid.csv"
  val GEO_LITE_CITY_DAT = ROOT_DIR + "GeoLiteCity.dat"
  val IP_DELAY_CSV = ROOT_DIR + "ip_delay.csv"

}
