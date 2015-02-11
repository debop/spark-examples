package learningsparkexamples.basic

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.google.gson.Gson
import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * ParseFunSuite
 * @author sunghyouk.bae@gmail.com
 */
class ParseFunSuite extends AbstractSparkFunSuite {

  test("parse csv") {
    val sc = new SparkContext("local", "BasicParseCsv", System.getenv("SPARK_HOME"))

    val inputFile = "files/favourite_animals.csv"
    val outputFile = "files/favourite_animals/output"
    val input = sc.textFile(inputFile)

    deleteDirectory(outputFile)

    val result: RDD[Array[String]] = input.map { (line: String) =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }

    val people: RDD[Person] = result.map(x => Person(x(0), x(1)))
    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")

    pandaLovers
    .map(person => List(person.name, person.favouriteAnimal).toArray)
    .mapPartitions { (people: Iterator[Array[String]]) =>
      val stringWriter = new StringWriter()
      val csvWriter = new CSVWriter(stringWriter)
      csvWriter.writeAll(people.toList.asJava)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)

    sc.stop()
  }

  test("parse json with gson") {

    val inputFile = "files/pandainfo.json"
    val outputFile = "files/pandainfo/output"
    deleteDirectory(outputFile)

    val sc = new SparkContext("local", "BasicParseJson", System.getenv("SPARK_HOME"))

    val input = sc.textFile(inputFile)

    val result: RDD[PersonPanda] = input.map { line =>
      println(s"line=$line")

      // 인스턴스를 spark action 안에서 정의해야 Serialization 예외가 발생하지 않는다.
      // 아마 map 이 병렬로 분산 실행된다면, 각각의 인스턴스가 필요하기 때문일 듯...
      val gson = JsonFactory.createGson()
      gson.fromJson(line, classOf[PersonPanda])
    }

    result
    .filter(_.lovesPandas)
    .map(JsonFactory.createGson().toJson(_))
    .saveAsTextFile(outputFile)

    sc.stop()
  }

  test("parse json with jackson") {
    //    val inputFile = "files/pandainfo2.json"
    //    val outputFile = "files/pandainfo/output2"
    //
    //    deleteDirectory(outputFile)
    //
    //    val sc = new SparkContext("local", "Parse Json With Jackson", System.getenv("SPARK_HOME"))
    //
    //    val input = sc.textFile(inputFile)
    //
    //    val result = input.flatMap { record =>
    //      try {
    //        val mapper = JsonFactory.createJacksonMapper()
    //        Some(mapper.readValue(record, classOf[PersonPanda]))
    //      } catch {
    //        case NonFatal(e) => None
    //      }
    //    }
    //
    //    result
    //    .filter(_.lovesPandas)
    //    .map { x =>
    //      val mapper = JsonFactory.createJacksonMapper()
    //      mapper.writeValueAsString(x)
    //    }.saveAsTextFile(outputFile)
    //
    //    sc.stop()
  }

  test("parse whole file csv") {
    val sc = new SparkContext("local", "BasicParseWholeFileCsv", System.getenv("SPARK_HOME"))

    val inputFile = "files/int_string.csv"
    val input = sc.wholeTextFiles(inputFile)

    val result: RDD[Array[String]] = input.flatMap {
      case (_, txt) =>
        val reader = new CSVReader(new StringReader(txt))
        reader.readAll().asScala
    }

    println(result.collect().map(_.toList).mkString(","))

    sc.stop()
  }
}

/**
 * Spark Action 내부에서 사용할 인스턴스의 경우 Object로 생성하도록 하면 된다.
 */
object JsonFactory {

  def createGson() = new Gson()

  def createJacksonMapper() = {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
  }
}


case class Person(name: String, favouriteAnimal: String)
case class PersonPanda(name: String, lovesPandas: Boolean)
