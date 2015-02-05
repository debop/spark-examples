package learningsparkexamples.basic

import com.datastax.spark.connector._
import learningsparkexamples.AbstractSparkFunSuite
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

// Cassandra 설치 후 (brew install cassandra)
// pip install cql
// cqlsh 실행 후 다음을 실행하면 test.kv 가 만들어진다.
// cqlsh>source 'files/cqlsh_setup';
//
class QueryCassandraFunSuite extends AbstractSparkFunSuite {

  test("query cassandra") {
    val cassandraHost = "localhost"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext("local", "QueryCassandra", conf)

    // entire table as an RDD
    // assumes your table test was created as CREATE TABLE test.kv(key text PRIMARY KEY, value int);
    val data = sc.cassandraTable("test", "kv")

    println("stats " + data.map(row => row.getInt("value")).stats())

    val rdd = sc.parallelize(List(("moremagic", 1)))
    rdd.saveToCassandra("test", "kv", SomeColumns("key", "value"))

    // save from a case class
    val otherRdd = sc.parallelize(List(KeyValue("magic", 0)))
    otherRdd.saveToCassandra("test", "kv")

    sc.stop()
  }
}

case class KeyValue(key: String, value: Integer)
