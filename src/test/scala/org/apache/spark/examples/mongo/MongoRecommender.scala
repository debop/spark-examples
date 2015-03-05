package org.apache.spark.examples.mongo

import java.util.Date

import com.mongodb.hadoop.MongoOutputFormat
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.examples.AbstractSparkExample
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.bson.BasicBSONObject

/**
 * data/ml-1m 의 사용자의 영화 선호도에 대해 추천 정보를 생성한 후 MongoDB에 저장한다.
 * @author sunghyouk.bae@gmail.com at 15. 3. 4.
 */
class MongoRecommender extends AbstractSparkExample {

  // private val log = LoggerFactory.getLogger(getClass)

  val HDFS_HOST = "data/ml-1m/"
  // "hdfs://localhost:9000"
  val MONGODB_HOST = "mongodb://127.0.0.1:27017/"

  sparkTest("mongo movielens recommendations") {

    val ratingsUri = HDFS_HOST + "ratings.dat"
    val usersUri = HDFS_HOST + "users.dat"
    val moviesUri = HDFS_HOST + "movies.dat"
    val mongodbUri = MONGODB_HOST + "movielens.predictions"

    println("load ratings...")

    val ratingsData: RDD[Rating] = sc.textFile(ratingsUri).map { line: String =>
      // UserId::MovieId::Rating::Timestamp
      println(s"ratings = $line")
      val elems = line.split("::")
      val userId = elems(0).toInt
      val movieId = elems(1).toInt
      val rating = elems(2).toDouble
      Rating(userId, movieId, rating)
    }.cache()

    println(s"ratings = ${ratingsData.count()}")

    println("load users ...")

    val usersData: RDD[Int] = sc.textFile(usersUri).map { line =>
      val elems = line.split("::")
      elems(0).toInt
    }.cache()

    println(s"users = ${usersData.count()}")

    println("load movies ...")

    val moviesData: RDD[Int] = sc.textFile(moviesUri).map { line =>
      val elems = line.split("::")
      elems(0).toInt
    }.cache()

    println(s"movies = ${moviesData.count()}")


    // generate complete pairing for all possible (user, movie) combinations
    val usersMovies: RDD[(Int, Int)] = usersData.cartesian(moviesData)

    println(s"usersMovies = ${usersMovies.count()}")


    println(s"train and calc predictions...")

    // create the model from existing ratings data
    val model: MatrixFactorizationModel = ALS.train(ratingsData, 5, 5, 0.01)

    val predictions: RDD[Rating] = model.predict(usersMovies).filter(r => r.rating > 4.5).cache()

    val predictionsOutput: RDD[(Null, BasicBSONObject)] = predictions.map { rating =>
      val doc = new BasicBSONObject()
      doc.put("userid", rating.user)
      doc.put("movieid", rating.product)
      doc.put("rating", rating.rating)
      doc.put("timestamp", new Date())

      (null, doc)
    }.cache()

    println(s"writing ${predictionsOutput.count()} documents to $mongodbUri")

    val predictionsConfig = new org.apache.hadoop.conf.Configuration()
    predictionsConfig.set("mongo.output.uri", mongodbUri)


    predictionsOutput.saveAsNewAPIHadoopFile("file:///notapplicable",
                                              classOf[Any],
                                              classOf[Any],
                                              classOf[MongoOutputFormat[Any, Any]],
                                              predictionsConfig)
  }

}
