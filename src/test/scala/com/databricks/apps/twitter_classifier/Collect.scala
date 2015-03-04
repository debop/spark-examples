package com.databricks.apps.twitter_classifier

import java.io.File

import com.google.gson.Gson
import org.apache.commons.io.FileUtils
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Collect
 * @author sunghyouk.bae@gmail.com at 15. 3. 3.
 */
class Collect extends AbstractSparkExample {

  var numTweetsCollected = 0L
  var partNum = 0
  val gson = new Gson()

  sparkTest("tweeter collect") {

    val outputDirectory = "data/tweeter/output"
    val numTweetsToCollect = 10
    val intervalSecs = 5
    val partitionsEachInterval = 5

    val outputDir = new File(outputDirectory)
    if (outputDir.exists()) {
      FileUtils.deleteDirectory(outputDir)
    }
    outputDir.mkdirs()

    val ssc = new StreamingContext(sc, Seconds(intervalSecs))

    val tweetStream: DStream[String] = TwitterUtils.createStream(ssc, Utils.getAuth).map(gson.toJson(_))

    tweetStream.foreachRDD { (rdd, time) =>
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          sys.exit(0)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
