package fastdataprocessing.pandaspark.examples

import org.apache.spark.SparkContext._

/**
 * LoadSequenceFile
 * @author sunghyouk.bae@gmail.com at 15. 3. 6.
 */
class LoadSequenceFile extends AbstractFastDataProcessing {

  sparkTest("load sequence file") {
    val inputFile = FAST_DATA_ROOT_DIR + "sequence"

    val outputRDD = sc.parallelize(List(("debop", 47), ("midoogi", 46)))
    outputRDD.saveAsSequenceFile(inputFile)

    val data = sc.sequenceFile[String, Int](inputFile)
    val concreteData = data.collect()

    println(concreteData.mkString("\n"))
  }

}
