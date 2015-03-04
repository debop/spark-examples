package com.databricks.apps.twitter_classifier

import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Pulls live tweets and filters them for tweets in the chosen cluster.
 *
 * @author sunghyouk.bae@gmail.com at 15. 3. 3.
 */
class Predict extends AbstractSparkExample {

  sparkTest("twitter predict") {

    val Array(modelFile, Utils.IntParam(clusterNumber)) = Utils.parseCommandLineWithTwitterCredentials(args)

    println("Initialize Streaming Spark Context...")
    val ssc = new StreamingContext(sc, Seconds(5))

    println("Initializing Twitter stream...")
    val tweets = TwitterUtils.createStream(ssc, Utils.getAuth)
    val statuses = tweets.map(_.getText)

    println("Initializing the KMeans model...")
    val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile.toString).collect())

    val filteredTweets: DStream[String] = statuses.filter(t => model.predict(Utils.featurize(t)) == clusterNumber)
    filteredTweets.print()

    println("Initialization complete.")
    ssc.start()
    ssc.awaitTermination()
  }

}
