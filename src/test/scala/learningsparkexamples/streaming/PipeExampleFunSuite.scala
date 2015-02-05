package learningsparkexamples.streaming

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext

/**
 * Created by debop on 15. 2. 5..
 */
class PipeExampleFunSuite extends AbstractSparkFunSuite {

  test("pipe example") {
    val sc = new SparkContext("local", "PipeExample", System.getenv("SPARK_HOME"))
    val rdd = sc.parallelize(Array(
                                    "37.75889318222431,-122.42683635321838,37.7614213,-122.4240097",
                                    "37.7519528,-122.4208689,37.8709087,-122.2688365"
                                  ))
  }

}
