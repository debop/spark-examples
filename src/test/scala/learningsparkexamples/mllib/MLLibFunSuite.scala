package learningsparkexamples.mllib

import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

class MLLibFunSuite extends AbstractSparkFunSuite {

  test("book example") {

    val conf = new SparkConf().setMaster("local").setAppName("Book example: Scala")
    val sc = new SparkContext(conf)

    // Load 2 types of emails from text files: spam and ham (non-spam)
    // Each line has text from one email.
    val spam: RDD[String] = sc.textFile("files/spam.txt")
    val ham: RDD[String] = sc.textFile("files/ham.txt")

    // Create a HashingTF instance to map email text to vectors of 100 features.
    val tf = new HashingTF()
    tf.setNumFeatures(100)

    // Each email is split into words, and each word is mapped to one feature.
    // MLLib 가 1.1.1 과 1.2.0 이 다르다.



    sc.stop()
  }

}
