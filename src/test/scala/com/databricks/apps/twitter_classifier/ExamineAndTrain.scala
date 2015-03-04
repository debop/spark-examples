package com.databricks.apps.twitter_classifier

import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SchemaRDD}

/**
 * Examine the collected tweets and trains a model based on them.
 *
 * @author sunghyouk.bae@gmail.com at 15. 3. 3.
 */
class ExamineAndTrain extends AbstractSparkExample {

  val jsonParsor = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  sparkTest("twetter examine and train") {

    val tweetInput = "data/tweet/"
    val outputModelDir = "data/tweet/examine/output"
    val numClusters = 3
    val numIterations = 10

    val sqlContext = new SQLContext(sc)

    val tweets: RDD[String] = sc.textFile(tweetInput)
    println("--------- Sample JSON Tweets -------------")
    tweets.take(5).foreach { tweet =>
      println(gson.toJson(jsonParsor.parse(tweet)))
    }

    val tweetTable: SchemaRDD = sqlContext.jsonFile(tweetInput).cache()
    tweetTable.registerTempTable("tweetTable")

    println("------- Tweet table Schema ---------")
    tweetTable.printSchema()

    println("------- Sample Tweet Text ----------")
    sqlContext.sql( """SELECT user.lang, user.name, text
                      |FROM tweetTable
                      |LIMIT 1000""".stripMargin).collect().foreach(println)

    println("---- Total count by languages Lang, count(*) ----")
    sqlContext.sql( """SELECT user.lang, COUNT(*) as cnt
                      |FROM tweetTable
                      |GROUP BY user.lang
                      |ORDER BY cnt DESC
                      |LIMIT 25""".stripMargin)
    .collect().foreach(println)

    val texts = sqlContext.sql("SELECT text from tweetTable").map(_.head.toString)
    // Cache the vectors RDD since it will be used for all the KMeans iterations.
    val vectors: RDD[Vector] = texts.map(Utils.featurize).cache()
    vectors.count() //Calls an action on the RDD to populate the vectors cache.

    val model: KMeansModel = KMeans.train(vectors, numClusters, numIterations)
    sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)

    val some_tweets = texts.take(100)
    println("---- Example tweets from the clusters")
    for (i <- 0 until numClusters) {
      println(s"\nCLUSTERS $i:")
      some_tweets.foreach { t =>
        if (model.predict(Utils.featurize(t)) == i) {
          println(t)
        }
      }
    }
  }

}
