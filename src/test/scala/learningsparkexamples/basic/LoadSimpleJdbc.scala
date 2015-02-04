package learningsparkexamples.basic

import java.sql.{ResultSet, DriverManager}
import javax.sql.DataSource

import learningsparkexamples.AbstractSparkFunSuite
import learningsparkexamples.jdbc.{JdbcDrivers, DataSources}
import org.apache.spark._
import org.apache.spark.rdd.JdbcRDD
import org.slf4j.LoggerFactory

import scala.slick.driver.H2Driver
import scala.util.Try
import scala.slick.profile._
import scala.slick.driver.JdbcProfile
import scala.slick.lifted._

/**
 * LoadSimpleJdbc
 * @author sunghyouk.bae@gmail.com
 */
class LoadSimpleJdbc extends AbstractSparkFunSuite {

  lazy val driver = scala.slick.driver.H2Driver
  lazy val profile = driver.profile

  import driver.simple._
  import JdbcHelper._

  test("load simple jdbc") {

    setupDatabase()

    val sc = new SparkContext("local", "Load Simple JDBC", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc,
                            () => getDataSource.getConnection,
                            "SELECT * FROM PANDA WHERE ? <= id AND ID <= ?",
                            lowerBound = 1,
                            upperBound = 3,
                            numPartitions = 2,
                            mapRow = extractValues)
    println("jdbc data=" + data.collect().toList)

    sc.stop()
  }
}

object JdbcHelper {

  private val log = LoggerFactory.getLogger(getClass)

  lazy val driver = scala.slick.driver.H2Driver
  lazy val profile = driver.profile

  import driver.simple._

  var dataSource : DataSource = _

  def setupDatabase(): Unit = {

    class Panda(tag: Tag) extends Table[(Int, String)](tag, "PANDA") {
      def id = column[Int]("ID")
      def name = column[String]("NAME")
      def * = (id, name)
    }

    def Pandas = TableQuery[Panda]

    val db = driver.simple.Database.forDataSource(getDataSource)

    db.withSession { implicit session =>
      Try {Pandas.ddl.drop}
      Pandas.ddl.create

      println(s"ddl=${Pandas.ddl.createStatements.mkString("\n")}")

      Pandas ++= Seq((1, "debop"), (2, "Sunghyouk"), (3, "Merry"), (4, "Karl"))
    }
    println(s"Finish to create database")
  }

  def getDataSource:DataSource = {
    if(dataSource == null) {
      dataSource = DataSources.getDataSource(JdbcDrivers.DRIVER_CLASS_H2, "jdbc:h2:mem:test", "sa", "")
    }
    dataSource
  }

  def extractValues(r: ResultSet) = (r.getInt(1), r.getString(2))
}
