package jobs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import mason.spark.configs.MergeConfig
import mason.spark.jobs.MergeJob
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.reflect.io.Directory
import java.io.File
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class MergeJobTest extends FunSuite with BeforeAndAfter with DataFrameSuiteBase {

  before {
    spark.sparkContext.setLogLevel("FATAL")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("spark").setLevel(Level.OFF)
  }

  after {
    val directory = new Directory(new File(".tmp"))
    directory.deleteRecursively()
  }

  def assertEquals(df: DataFrame, data: String) = {
    val splitted = data.split("\n")
    val a = splitted
      .zipWithIndex.filter{r => (Array(0,1,2,3,splitted.length,splitted.length - 1, splitted.length - 2).contains(r._2) == false)}
      .map{r => r._1.stripPrefix("|").stripSuffix("|").split('|').map(_.trim()).toSeq.mkString(",")}.toSeq.sorted.mkString(",")

    // Strictly a stringly test for now
    val b = df.collect().map{r =>
      r.toSeq.map{a => if (a == null) { "null" } else { a.toString() } }.mkString(",")
    }.toSeq.sorted.mkString(",")
    assert(a == b)
  }

  test("valid csv test") {
    val config = new MergeConfig("src/test/resources/test_csv/", "text-csv", ".tmp/merged/")

    MergeJob.run(config)

    val mergedDF = spark.read.parquet(".tmp/merged")
    val expect = """
    +-------+-----+
    |   type|price|
    +-------+-----+
    | wrench| 20.0|
    | hammer| 10.0|
    |wrench2| 19.0|
    |wrench3| 14.0|
    |wrench4| 24.0|
    |wrench5| 30.0|
    |hammer2|  9.0|
    |hammer3|  5.0|
    |hammer4| 12.0|
    |hammer5| 20.0|
    | wrench| 20.0|
    | hammer| 10.0|
    |wrench2| 19.0|
    +-------+-----+
    """.stripMargin

    assertEquals(mergedDF, expect)
  }

  test("extract path") {
    val config = new MergeConfig("src/test/resources/test_csv_path/", "text-csv", ".tmp/merged/",true)

    MergeJob.run(config)

    val mergedDF = spark.read.parquet(".tmp/merged").select("type", "price", "dir_0")

    val expect = """
    +-------+-----+-------------+
    |type   |price|dir_0        |
    +-------+-----+-------------+
    |wrench |20.0 |manufacturer1|
    |hammer |10.0 |manufacturer1|
    |wrench2|19.0 |manufacturer1|
    |wrench3|14.0 |manufacturer1|
    |wrench4|24.0 |manufacturer1|
    |wrench5|30.0 |manufacturer1|
    |hammer2|9.0  |manufacturer1|
    |hammer3|5.0  |manufacturer1|
    |hammer4|12.0 |manufacturer1|
    |hammer5|20.0 |manufacturer1|
    |wrench |20.0 |manufacturer2|
    |hammer |10.0 |manufacturer2|
    |wrench2|19.0 |manufacturer2|
    +-------+-----+-------------+
    """.stripMargin

    assertEquals(mergedDF, expect)
  }

  test("valid parquet") {

    val config = new MergeConfig("src/test/resources/test_parquet/", "parquet", ".tmp/merged/")

    MergeJob.run(config)

    val mergedDF = spark.read.parquet(".tmp/merged")

    val expect = """
    +--------+-------+------------+--------+
    |  widget|  price|manufacturer|in_stock|
    +--------+-------+------------+--------+
    | tractor|10000.0|   John Deer|    true|
    |forklift| 4000.0|      Bobcat|    true|
    |   crane| 5000.0|        Case|   false|
    |  casing|   30.0|        null|    null|
    |  wrench|   20.0|        null|    null|
    |    bolt|   25.0|        null|    null|
    +--------+-------+------------+--------+
    """.stripMargin

    assertEquals(mergedDF, expect)

  }

  test("invalid csv") {

    val config = new MergeConfig("src/test/resources/test_bad_csv/", "text-csv", ".tmp/merged/")
    MergeJob.run(config)
    val mergedDF = spark.read.parquet(".tmp/merged")
    val expect = """
    +--------------+-----+
    |          type|price|
    +--------------+-----+
    |        wrench| 20.0|
    |        hammer| 10.0|
    |       wrench2| 19.0|
    |       wrench3| 14.0|
    |       wrench4| 24.0|
    |       wrench5| 30.0|
    |       hammer2|  9.0|
    |       hammer3|  5.0|
    |       hammer4| 12.0|
    |       hammer5| 20.0|
    |asdflkajsdlkfj| null|
    |          asdf| null|
    +--------------+-----+
    """.stripMargin

    assertEquals(mergedDF, expect)
  }

  test("valid json") {

    val config = new MergeConfig("src/test/resources/test_json/", "json", ".tmp/merged/")
    MergeJob.run(config)
    val mergedDF = spark.read.parquet(".tmp/merged")
    val expect = """
    +------+------+------+------+------+------+
    |field1|field2|field3|field4|field5|field6|
    +------+------+------+------+------+------+
    |  test| test2| test3|  null|  null|  null|
    |  test|  null| test3|  null|  null| test2|
    |  null|  null|  test| test2| test3|  null|
    +------+------+------+------+------+------+
    """.stripMargin

    assertEquals(mergedDF, expect)

  }

  test("valid jsonl") {

    val config = new MergeConfig("src/test/resources/test_jsonl/", "jsonl", ".tmp/merged/")
    MergeJob.run(config)
    val mergedDF = spark.read.parquet(".tmp/merged")
    val expect = """
    +------+------+------+------+------+------+------+------+------+-----+------+------+
    |field1|field2|field3|field4|field5|field6|field7|field8|field9|other|other2|other3|
    +------+------+------+------+------+------+------+------+------+-----+------+------+
    |  test| test2| test3|  null|  null|  null|  null|  null|  null| null|  null|  null|
    |  null|  null|  test| test2| test3|  null|  null|  null|  null| null|  null|  null|
    |  test|  null|  null|  null|  null| test2| test3|  null|  null| null|  null|  null|
    |  null|  null|  null|  null|  null|  null|  test| test2| test3| null|  null|  null|
    |  null|  null|  null|  null|  null|  null|  null|  null|  null| test| test2| test3|
    +------+------+------+------+------+------+------+------+------+-----+------+------+
    """.stripMargin
    assertEquals(mergedDF, expect)

  }

  test("complex json") {
    val config = new MergeConfig("src/test/resources/test_json_complex/", "json", ".tmp/merged/")
    MergeJob.run(config)
    val mergedDF = spark.read.parquet(".tmp/merged")
    val expect = """
    +----------------------------------------------------------------------------------------------------------------------------+-----+------+------+
    |data                                                                                                                        |field|field2|field3|
    +----------------------------------------------------------------------------------------------------------------------------+-----+------+------+
    |WrappedArray([test,test2,null,null,null,null], [test,5,test3,test4,null,null], [null,null,test3,test4,[going on here],null])|null |null  |null  |
    |WrappedArray([test,test2,null,null,null,null], [test,5,test3,test4,null,null], [null,null,test3,test4,null,things])         |null |null  |null  |
    |null                                                                                                                        |test |test2 |test3 |
    +----------------------------------------------------------------------------------------------------------------------------+-----+------+------+
    """.stripMargin
    assertEquals(mergedDF, expect)

  }

}
