package org.apache.spark.examples.mllib


import java.io.File

import com.google.common.io.Files
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * An example of how to use [[org.apache.spark.sql.SchemaRDD]] as a Dataset for ML. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DatasetExample [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
class DatasetExample extends AbstractSparkExample {

  case class Params(input: String = "data/mllib/sample_libsvm_data.txt",
                    dataFormat: String = "libsvm") extends AbstractParams[Params]


  sparkTest("Dataset example - libsvm format") {
    run(Params())
  }

  def run(params: Params): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext._

    val origData: RDD[LabeledPoint] = params.dataFormat match {
      case "dense" => MLUtils.loadLabeledPoints(sc, params.input)
      case "libsvm" => MLUtils.loadLibSVMFile(sc, params.input)
    }
    println(s"Loaded ${origData.count()} instances from file: ${params.input}")

    // Convert input data to SchemaRDD explicitly.
    val schemaRDD: SchemaRDD = origData
    println(s"Inferred schema:\n${schemaRDD.schema.prettyJson}")
    println(s"Converted to SchemaRDD with ${schemaRDD.count()} records")

    // Select columns, using implicit conversion to SchemaRDD.
    val labelsSchemaRDD: SchemaRDD = origData.select('label)
    val labels: RDD[Double] = labelsSchemaRDD.map { case Row(v: Double) => v}
    val numLabels = labels.count()
    val meanLabel = labels.fold(0.0)(_ + _) / numLabels
    println(s"Selected label column with average value $meanLabel")

    val featuresSchemaRDD: SchemaRDD = origData.select('features)
    val features: RDD[Vector] = featuresSchemaRDD.map { case Row(v: Vector) => v}
    val featureSummary =
      features.aggregate(new MultivariateOnlineSummarizer())(
                                                              (summary, feat) => summary.add(feat),
                                                              (sum1, sum2) => sum1.merge(sum2)
                                                            )
    println(s"Selected features column with average values:\n ${featureSummary.mean.toString}")

    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()

    val outputDir = new File(tmpDir, "dataset").toString
    println(s"Saving to $outputDir as Parquet file.")
    schemaRDD.saveAsParquetFile(outputDir)

    println(s"Loading Parquet file with UDT from $outputDir.")
    val newDataset = sqlContext.parquetFile(outputDir)

    println(s"Schema from Parquet: ${newDataset.schema.prettyJson}")
    val newFeatures = newDataset.select('features).map { case Row(v: Vector) => v}
    val newFeaturesSummary = newFeatures
                             .aggregate(new MultivariateOnlineSummarizer())(
        (summary, feat) => summary.add(feat),
        (sum1, sum2) => sum1.merge(sum2))

    println(s"Selected features column with average values:\n${newFeaturesSummary.mean.toString}")
  }

}
