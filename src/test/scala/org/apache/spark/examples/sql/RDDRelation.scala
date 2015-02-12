package org.apache.spark.examples.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.examples.AbstractSparkExample

case class Record(key: Int, value: String)

class RDDRelation extends AbstractSparkExample {

  test("RDD Relation") {
    val conf = new SparkConf().setMaster("local").setAppName("RDDRelation")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext._

    val rdd: RDD[Record] = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    rdd.registerTempTable("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    sql("SELECT * FROM records").collect().foreach(println)

    val count = sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql: SchemaRDD = sql("SELECT key, value from records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    rdd.where('key === 1).orderBy('value.asc).select('key).collect().foreach(println)

    // Write out an RDD as a parquet file.
    deleteDirectory("pair.parquet")
    rdd.saveAsParquetFile("pair.parquet")

    // Read in parquet file.  Parquet files are self-describing so the schmema is preserved.
    val parquetFile = sqlContext.parquetFile("pair.parquet")

    // Queries can be run using the DSL on parequet files just like the original RDD.
    parquetFile.where('key === 1).select('value as 'a).collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerTempTable("parquetTable")
    sql("SELECT * FROM parquetTable").collect().foreach(println)

    sc.stop()
  }
}
