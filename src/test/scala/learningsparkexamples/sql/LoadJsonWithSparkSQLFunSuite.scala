package learningsparkexamples.sql

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class LoadJsonWithSparkSQLFunSuite extends AbstractSparkFunSuite {

  test("load json with SparkSQL") {
    val inputFile = "files/pandainfo.json"

    val sc = new SparkContext("local", "LoadJsonWithSparkSQL", System.getenv("SPARK_HOME"))
    val sqlCtx = new SQLContext(sc)
    val input = sqlCtx.jsonFile(inputFile)
    input.printSchema()
  }

}
