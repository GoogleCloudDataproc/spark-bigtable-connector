/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigtable

import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.admin.v2.{BigtableTableAdminClient, BigtableTableAdminSettings}
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry
import com.google.cloud.bigtable.data.v2.{BigtableDataClient, BigtableDataSettings}
import com.google.cloud.bigtable.emulator.v2.Emulator
import com.google.cloud.spark.bigtable.datasources._
import com.google.protobuf.ByteString
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class RowFilterLogicTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {
  @transient var sc: SparkContext = null

  val t1TableName = "t1"
  val t2TableName = "t2"
  val t3TableName = "t3"
  val columnFamily = "c"

  val timestamp = 1234567890000L

  var sqlContext: SQLContext = null
  var df: DataFrame = null

  var emulator: Emulator = null

  override def beforeAll() {
    emulator = Emulator.createBundled()
    emulator.start()

    logInfo(" - creating table " + t1TableName)
    createTable(t1TableName, columnFamily)
    logInfo(" - created table")
    logInfo(" - creating table " + t2TableName)
    createTable(t2TableName, columnFamily)
    logInfo(" - created table")
    logInfo(" - creating table " + t3TableName)
    createTable(t3TableName, columnFamily)
    logInfo(" - created table")

    val sparkConf = new SparkConf

    sc = new SparkContext("local", "test", sparkConf)

    fillTables()

    def bigtableTable1Catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"}
            |}
          |}""".stripMargin

    sqlContext = new SQLContext(sc)

    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> bigtableTable1Catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )

    df.registerTempTable("bigtableTable1")

    def bigtableTable2Catalog = s"""{
            |"table":{"name":"t2"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"long"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"}
            |}
          |}""".stripMargin

    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> bigtableTable2Catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )

    df.registerTempTable("bigtableTable2")
  }

  override def afterAll() {
    emulator.stop()

    sc.stop()
  }

  /** A example of query three fields and also only using rowkey points for the filter
    */
  test("Test rowKey point only rowKey query") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "(KEY_FIELD = 'get1' or KEY_FIELD = 'get2' or KEY_FIELD = 'get3')"
      )
      .take(10)

    assert(results.length == 3)
  }

  /** A example of query three fields and also only using cell points for the filter
    */
  test("Test cell point only rowKey query") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "(B_FIELD = '4' or B_FIELD = '10' or A_FIELD = 'foo1')"
      )
      .take(10)

    assert(results.length == 3)
  }

  /** A example of a OR merge between to ranges the result is one range
    * Also an example of less then and greater then
    */
  test("Test two range rowKey query") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "( KEY_FIELD < 'get2' or KEY_FIELD > 'get3')"
      )
      .take(10)

    assert(results.length == 3)
  }

  /** A example of a OR merge between to ranges the result is one range
    * Also an example of less then and greater then
    *
    * This example makes sure the code works for a long rowKey
    */
  test(
    "Test two range rowKey query where the rowKey is Long and there is a range over lap"
  ) {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable2 " +
          "WHERE " +
          "( KEY_FIELD < 4 or KEY_FIELD > 2)"
      )
      .take(10)

    assert(results.length == 5)
  }

  /** A example of a OR merge between to ranges the result is two ranges
    * Also an example of less then and greater then
    *
    * This example makes sure the code works for a long rowKey
    */
  test(
    "Test two range rowKey query where the rowKey is Long and the ranges don't over lap"
  ) {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable2 " +
          "WHERE " +
          "( KEY_FIELD < 2 or KEY_FIELD > 4)"
      )
      .take(10)

    assert(results.length == 2)
  }

  /** A example of a AND merge between to ranges the result is one range
    * Also an example of less then and equal to and greater then and equal to
    */
  test("Test one combined range rowKey query") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')"
      )
      .take(10)
    assert(results.length == 2)
  }

  /** A example of a AND merge between to ranges the result is one range on
    * a long row key.
    * Also an example of less then and equal to and greater then and equal to
    */
  test("Test one combined range rowKey query for long row key") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable2 " +
          "WHERE " +
          "(KEY_FIELD <= 3 and KEY_FIELD >= 2)"
      )
      .take(10)
    assert(results.length == 2)
  }

  /** Do a select with no filters
    */
  test("Test select only query") {
    val results = df.select("KEY_FIELD").take(10)
    assert(results.length == 5)
  }

  /** A complex query with one point and one range for both the
    * rowKey and the a column
    */
  test("Test SQL point and range combo") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "(KEY_FIELD = 'get1' and B_FIELD < '3') or " +
          "(KEY_FIELD >= 'get3' and B_FIELD = '8')"
      )
      .take(5)
    assert(results.length == 3)
  }

  /** A complex query with two complex ranges that doesn't merge into one
    */
  test("Test two complete range non merge rowKey query") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable2 " +
          "WHERE " +
          "( KEY_FIELD >= 1 and KEY_FIELD <= 2) or" +
          "( KEY_FIELD > 3 and KEY_FIELD <= 5)"
      )
      .take(10)
    assert(results.length == 4)
  }

  /** A complex query with two complex ranges that does merge into one
    */
  test("Test two complete range merge rowKey query") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get2') or" +
          "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')"
      )
      .take(10)
    assert(results.length == 4)
  }

  test("Test OR logic with a one RowKey and One column") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "( KEY_FIELD >= 'get1' or A_FIELD <= 'foo2') or" +
          "( KEY_FIELD > 'get3' or B_FIELD <= '4')"
      )
      .take(10)
    assert(results.length == 5)
  }

  test("Test OR logic with a two columns") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "( B_FIELD > '4' or A_FIELD <= 'foo2') or" +
          "( A_FIELD > 'foo2' or B_FIELD < '4')"
      )
      .take(10)
    assert(results.length == 5)
  }

  test("Test single RowKey Or Column logic") {
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableTable1 " +
          "WHERE " +
          "( KEY_FIELD >= 'get4' or A_FIELD <= 'foo2' )"
      )
      .take(10)
    assert(results.length == 4)
  }

  test("Test table that doesn't exist") {
    val catalog = s"""{
            |"table":{"name":"t1NotThere"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"c", "type":"string"}
            |}
          |}""".stripMargin
    intercept[Exception] {
      df = sqlContext.load(
        "bigtable",
        Map(
          "catalog" -> catalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString
        )
      )
      df.registerTempTable("bigtableNonExistingTmp")
      sqlContext
        .sql(
          "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableNonExistingTmp " +
            "WHERE " +
            "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get3') or" +
            "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')"
        )
        .count()
    }
  }

  test("Test table with column that doesn't exist") {
    val catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"C_FIELD":{"cf":"c", "col":"c", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )
    df.registerTempTable("bigtableFactColumnTmp")
    val result = sqlContext.sql(
      "SELECT KEY_FIELD, " +
        "B_FIELD, A_FIELD FROM bigtableFactColumnTmp"
    )
    assert(result.count() == 5)
  }

  test("Test table with LONG column") {
    val catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"L_FIELD":{"cf":"c", "col":"i", "type":"long"}
            |}
          |}""".stripMargin
    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )
    df.registerTempTable("bigtableLongTmp")
    val result = sqlContext.sql(
      "SELECT KEY_FIELD, B_FIELD, L_FIELD FROM bigtableLongTmp" +
        " where L_FIELD > 4 and L_FIELD < 10"
    )
    val localResult = result.take(5)
    assert(localResult.length == 2)
    assert(localResult(0).getLong(2) == 8)
  }

  // We don't throw an exception in this case since we just convert the bytes
  // in Bigtable to Long, even though they were originally intended as Strings.
  test("Test table with LONG column defined at wrong type") {
    val catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"L_FIELD":{"cf":"c", "col":"i", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )
    df.registerTempTable("bigtableLongWrongTypeTmp")
    val result = sqlContext.sql(
      "SELECT KEY_FIELD, " +
        "B_FIELD, L_FIELD FROM bigtableLongWrongTypeTmp"
    )
    val localResult = result.take(10)
    assert(localResult.length == 5)
    assert(localResult(0).getString(2).length == 8)
    assert(localResult(0).getString(2).charAt(0).toByte == 0)
    assert(localResult(0).getString(2).charAt(1).toByte == 0)
    assert(localResult(0).getString(2).charAt(2).toByte == 0)
    assert(localResult(0).getString(2).charAt(3).toByte == 0)
    assert(localResult(0).getString(2).charAt(4).toByte == 0)
    assert(localResult(0).getString(2).charAt(5).toByte == 0)
    assert(localResult(0).getString(2).charAt(6).toByte == 0)
    assert(localResult(0).getString(2).charAt(7).toByte == 1)
  }

  // In this an exception is expected since "FOOBAR" is not a valid type.
  test("Test bad column type") {
    val catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"FOOBAR"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"I_FIELD":{"cf":"c", "col":"i", "type":"string"}
            |}
          |}""".stripMargin
    intercept[Exception] {
      df = sqlContext.load(
        "bigtable",
        Map(
          "catalog" -> catalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString
        )
      )
      df.registerTempTable("bigtableLongWrongTypeTmp")
      val result = sqlContext.sql(
        "SELECT KEY_FIELD, " +
          "B_FIELD, I_FIELD FROM bigtableLongWrongTypeTmp"
      )
      val localResult = result.take(10)
      assert(localResult.length == 5)
    }
  }

  test("Test table with sparse column") {
    val catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"Z_FIELD":{"cf":"c", "col":"z", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )
    df.registerTempTable("bigtableZTmp")
    val result =
      sqlContext.sql("SELECT KEY_FIELD, B_FIELD, Z_FIELD FROM bigtableZTmp")
    val localResult = result.take(10)
    assert(localResult.length == 5)
    assert(localResult(0).getString(2) == null)
    assert(localResult(1).getString(2) == "FOO")
    assert(localResult(2).getString(2) == null)
    assert(localResult(3).getString(2) == "BAR")
    assert(localResult(4).getString(2) == null)
  }

  test("Test with column logic disabled") {
    val catalog = s"""{
            |"table":{"name":"t1"},
            |"rowkey":"key",
            |"columns":{
              |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
              |"A_FIELD":{"cf":"c", "col":"a", "type":"string"},
              |"B_FIELD":{"cf":"c", "col":"b", "type":"string"},
              |"Z_FIELD":{"cf":"c", "col":"z", "type":"string"}
            |}
          |}""".stripMargin
    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString,
        "spark.bigtable.push.down.row.key.filters" -> "false"
      )
    )
    df.registerTempTable("bigtableNoPushDownTmp")
    val results = sqlContext
      .sql(
        "SELECT KEY_FIELD, B_FIELD, A_FIELD FROM bigtableNoPushDownTmp " +
          "WHERE " +
          "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')"
      )
      .take(10)
    assert(results.length == 2)
  }

  test("Test mapping") {
    val catalog = s"""{
                     |"table":{"name":"t3"},
                     |"rowkey":"key",
                     |"columns":{
                     |"KEY_FIELD":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"BINARY_FIELD":{"cf":"c", "col":"binary", "type":"binary"},
                     |"LONG_FIELD":{"cf":"c", "col":"long", "type":"long"},
                     |"STRING_FIELD":{"cf":"c", "col":"string", "type":"string"}
                     |}
                     |}""".stripMargin
    df = sqlContext.load(
      "bigtable",
      Map(
        "catalog" -> catalog,
        "spark.bigtable.project.id" -> "fake-project",
        "spark.bigtable.instance.id" -> "fake-instance",
        "spark.bigtable.emulator.port" -> emulator.getPort().toString
      )
    )
    df.registerTempTable("bigtableTestMapping")
    val results = sqlContext
      .sql(
        "SELECT binary_field, " +
          "long_field, " +
          "string_field FROM bigtableTestMapping"
      )
      .collect()
    assert(results.length == 1)
    val result = results(0)
    System.out.println("row: " + result)
    System.out.println("0: " + result.get(0))
    System.out.println("1: " + result.get(1))
    System.out.println("2: " + result.get(2))
    assert(
      result
        .get(0)
        .asInstanceOf[Array[Byte]]
        .sameElements(Array(1.toByte, 2.toByte, 3.toByte))
    )
    assert(result.get(1) == 10000000000L)
    assert(result.get(2) == "string")
  }

  def writeCatalog = s"""{
                    |"table":{"name":"table1"},
                    |"rowkey":"key",
                    |"columns":{
                    |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                    |"col1":{"cf":"cf1", "col":"col1", "type":"bigint"},
                    |"col2":{"cf":"cf2", "col":"col2", "type":"string"}
                    |}
                    |}""".stripMargin
  def withCatalog(cat: String): DataFrame = {
    sqlContext.read
      .options(
        Map(
          "catalog" -> cat,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString
        )
      )
      .format("bigtable")
      .load()
  }

  test("populate table") {
    val sql = sqlContext
    import sql.implicits._
    val data = (0 to 255).map { i =>
      BigtableRecord(i, "extra")
    }
    sc.parallelize(data)
      .toDF
      .write
      .options(
        Map(
          "catalog" -> writeCatalog,
          "spark.bigtable.create.new.table" -> "true",
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "spark.bigtable.create.new.table" -> "true"
        )
      )
      .format("bigtable")
      .save()
  }

  test("empty column") {
    val df = withCatalog(writeCatalog)
    df.registerTempTable("table0")
    val c = sqlContext
      .sql("select count(1) from table0")
      .rdd
      .collect()(0)(0)
      .asInstanceOf[Long]
    assert(c == 256)
  }

  test("full query") {
    val df = withCatalog(writeCatalog)
    df.show()
    assert(df.count() == 256)
  }

  test("filtered query0") {
    val sql = sqlContext
    import sql.implicits._
    val df = withCatalog(writeCatalog)
    val s = df
      .filter($"col0" <= "row005")
      .select("col0", "col1")
    s.show()
    assert(s.count() == 6)
  }

  test("filtered query01") {
    val sql = sqlContext
    val df = withCatalog(writeCatalog)
    val s = df
      .filter(col("col0").startsWith("row00"))
      .select("col0", "col1")
    s.show()
    assert(s.count() == 10)
  }

  test("startsWith filtered query 1") {
    val sql = sqlContext
    val df = withCatalog(writeCatalog)
    val s = df
      .filter(col("col0").startsWith("row005"))
      .select("col0", "col1")
    s.show()
    assert(s.count() == 1)
  }

  test("startsWith filtered query 2") {
    val sql = sqlContext
    val df = withCatalog(writeCatalog)
    val s = df
      .filter(col("col0").startsWith("row"))
      .select("col0", "col1")
    s.show()
    assert(s.count() == 256)
  }

  test("startsWith filtered query 3") {
    val sql = sqlContext
    val df = withCatalog(writeCatalog)
    val s = df
      .filter(col("col0").startsWith("row19"))
      .select("col0", "col1")
    s.show()
    assert(s.count() == 10)
  }

  test("startsWith filtered query 4") {
    val sql = sqlContext
    val df = withCatalog(writeCatalog)
    val s = df
      .filter(col("col0").startsWith(""))
      .select("col0", "col1")
    s.show()
    assert(s.count() == 256)
  }

  test("Timestamp semantics") {
    val sql = sqlContext
    import sql.implicits._
    // There's already some data in here from recently. Let's throw something in
    // from 1993 which we can include/exclude and add some data with the implicit (now) timestamp.
    // Then we should be able to cross-section it and only get points in between, get the most recent view
    // and get an old view.
    val oldMs = 754869600000L
    val startMs = System.currentTimeMillis()
    val oldData = (0 to 100).map { i =>
      BigtableRecord(i, "old")
    }
    val newData = (200 to 255).map { i =>
      BigtableRecord(i, "new")
    }
    sc.parallelize(oldData)
      .toDF
      .write
      .options(
        Map(
          "catalog" -> writeCatalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "spark.bigtable.write.timestamp.milliseconds" -> oldMs.toString
        )
      )
      .format("bigtable")
      .save()
    sc.parallelize(newData)
      .toDF
      .write
      .options(
        Map(
          "catalog" -> writeCatalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString
        )
      )
      .format("bigtable")
      .save()
    // Test getting everything -- Full Scan, No range
    val everything = sqlContext.read
      .options(
        Map(
          "catalog" -> writeCatalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString
        )
      )
      .format("bigtable")
      .load()
    assert(everything.count() == 256)
    // Test getting everything -- Pruned Scan, TimeRange
    val element50 = everything
      .where(col("col0") === lit("row050"))
      .select("col2")
      .collect()(0)(0)
    assert(element50 == "String50: extra")
    val element200 = everything
      .where(col("col0") === lit("row200"))
      .select("col2")
      .collect()(0)(0)
    assert(element200 == "String200: new")
    // Test Getting old stuff -- Full Scan, TimeRange
    val oldRange = sqlContext.read
      .options(
        Map(
          "catalog" -> writeCatalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "spark.bigtable.read.timerange.start.milliseconds" -> "0",
          "spark.bigtable.read.timerange.end.milliseconds" -> (oldMs + 100).toString
        )
      )
      .format("bigtable")
      .load()
    assert(oldRange.count() == 101)
    // Test Getting old stuff -- Pruned Scan, TimeRange
    val oldElement50 = oldRange
      .where(col("col0") === lit("row050"))
      .select("col2")
      .collect()(0)(0)
    assert(oldElement50 == "String50: old")
    // Test Getting middle stuff -- Full Scan, TimeRange
    val middleRange = sqlContext.read
      .options(
        Map(
          "catalog" -> writeCatalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "spark.bigtable.read.timerange.start.milliseconds" -> "0",
          "spark.bigtable.read.timerange.end.milliseconds" -> (startMs).toString
        )
      )
      .format("bigtable")
      .load()
    assert(middleRange.count() == 256)
    // Test Getting middle stuff -- Pruned Scan, TimeRange
    val middleElement200 = middleRange
      .where(col("col0") === lit("row200"))
      .select("col2")
      .collect()(0)(0)
    assert(middleElement200 == "String200: extra")
  }
  // catalog for insertion
  def avroWriteCatalog = s"""{
                             |"table":{"name":"avrotable"},
                             |"rowkey":"key",
                             |"columns":{
                             |"col0":{"cf":"rowkey", "col":"key", "type":"binary"},
                             |"col1":{"cf":"cf1", "col":"col1", "type":"binary"}
                             |}
                             |}""".stripMargin
  // catalog for read
  def avroCatalog = s"""{
                        |"table":{"name":"avrotable"},
                        |"rowkey":"key",
                        |"columns":{
                        |"col0":{"cf":"rowkey", "col":"key",  "avro":"avroSchema"},
                        |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                        |}
                        |}""".stripMargin
  // for insert to another table
  def avroCatalogInsert = s"""{
                              |"table":{"name":"avrotableInsert"},
                              |"rowkey":"key",
                              |"columns":{
                              |"col0":{"cf":"rowkey", "col":"key", "avro":"avroSchema"},
                              |"col1":{"cf":"cf1", "col":"col1", "avro":"avroSchema"}
                              |}
                              |}""".stripMargin
  def withAvroCatalog(cat: String): DataFrame = {
    sqlContext.read
      .options(
        Map(
          "avroSchema" -> AvroBigtableKeyRecord.schemaString,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "catalog" -> avroCatalog
        )
      )
      .format("bigtable")
      .load()
  }

  test("populate avro table") {
    val sql = sqlContext
    import sql.implicits._
    val data = (0 to 255).map { i =>
      AvroBigtableKeyRecord(i)
    }
    sc.parallelize(data)
      .toDF
      .write
      .options(
        Map(
          "catalog" -> avroWriteCatalog,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "spark.bigtable.create.new.table" -> "true"
        )
      )
      .format("bigtable")
      .save()
  }

  test("avro empty column") {
    val df = withAvroCatalog(avroCatalog)
    df.registerTempTable("avrotable")
    val c = sqlContext
      .sql("select count(1) from avrotable")
      .rdd
      .collect()(0)(0)
      .asInstanceOf[Long]
    assert(c == 256)
  }

  test("avro full query") {
    val df = withAvroCatalog(avroCatalog)
    df.show()
    df.printSchema()
    assert(df.count() == 256)
  }

  test("avro serialization and deserialization query") {
    val df = withAvroCatalog(avroCatalog)
    df.write
      .options(
        Map(
          "avroSchema" -> AvroBigtableKeyRecord.schemaString,
          "spark.bigtable.project.id" -> "fake-project",
          "spark.bigtable.instance.id" -> "fake-instance",
          "spark.bigtable.emulator.port" -> emulator.getPort().toString,
          "catalog" -> avroCatalogInsert,
          "spark.bigtable.create.new.table" -> "true"
        )
      )
      .format("bigtable")
      .save()
    val newDF = withAvroCatalog(avroCatalogInsert)
    newDF.show()
    newDF.printSchema()
    assert(newDF.count() == 256)
  }

  test("avro filtered query") {
    val sql = sqlContext
    import sql.implicits._
    val df = withAvroCatalog(avroCatalog)
    val r = df
      .filter($"col1.name" === "name005" || $"col1.name" <= "name005")
      .select("col0", "col1.favorite_color", "col1.favorite_number")
    r.show()
    assert(r.count() == 6)
  }

  test("avro Or filter") {
    val sql = sqlContext
    import sql.implicits._
    val df = withAvroCatalog(avroCatalog)
    val s = df
      .filter($"col1.name" <= "name005" || $"col1.name".contains("name007"))
      .select("col0", "col1.favorite_color", "col1.favorite_number")
    s.show()
    assert(s.count() == 7)
  }

  private def createTable(tableName: String, colFamilyName: String): Unit = {
    val settingsBuilder: BigtableTableAdminSettings.Builder =
      BigtableTableAdminSettings
        .newBuilderForEmulator(emulator.getPort())
        .setProjectId("fake-project")
        .setInstanceId("fake-instance")
    val adminClient: BigtableTableAdminClient =
      BigtableTableAdminClient.create(settingsBuilder.build())
    val createTableRequest =
      CreateTableRequest
        .of(tableName)
        .addFamily(colFamilyName)
    adminClient.createTable(createTableRequest)
    adminClient.close()
  }

  private def fillTables(): Unit = {
    var settingsBuilder: BigtableDataSettings.Builder =
      BigtableDataSettings
        .newBuilderForEmulator(emulator.getPort())
        .setProjectId("fake-project")
        .setInstanceId("fake-instance")
    var dataClient = BigtableDataClient.create(settingsBuilder.build())

    var batcher = dataClient.newBulkMutationBatcher(t1TableName)
    var mutation = RowMutationEntry
      .create("get1")
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo1"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("1"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(1L))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create("get2")
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo2"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("4"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(4L))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("z")),
        ByteString.copyFrom(BytesConverter.toBytes("FOO"))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create("get3")
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo3"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("8"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(8L))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create("get4")
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo4"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("10"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(10L))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("z")),
        ByteString.copyFrom(BytesConverter.toBytes("BAR"))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create("get5")
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo5"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("8"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(8L))
      )
    batcher.add(mutation)
    batcher.flush()
    batcher.close()

    batcher = dataClient.newBulkMutationBatcher(t2TableName)
    mutation = RowMutationEntry
      .create(ByteString.copyFrom(BytesConverter.toBytes(1L)))
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo1"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("1"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(1L))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create(ByteString.copyFrom(BytesConverter.toBytes(2L)))
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo2"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("4"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(4L))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("z")),
        ByteString.copyFrom(BytesConverter.toBytes("FOO"))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create(ByteString.copyFrom(BytesConverter.toBytes(3L)))
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo3"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("8"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(8L))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create(ByteString.copyFrom(BytesConverter.toBytes(4L)))
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo4"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("10"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(10L))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("z")),
        ByteString.copyFrom(BytesConverter.toBytes("BAR"))
      )
    batcher.add(mutation)
    mutation = RowMutationEntry
      .create(ByteString.copyFrom(BytesConverter.toBytes(5L)))
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("a")),
        ByteString.copyFrom(BytesConverter.toBytes("foo5"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("b")),
        ByteString.copyFrom(BytesConverter.toBytes("8"))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("i")),
        ByteString.copyFrom(BytesConverter.toBytes(8L))
      )
    batcher.add(mutation)
    batcher.flush()
    batcher.close()

    batcher = dataClient.newBulkMutationBatcher(t3TableName)
    mutation = RowMutationEntry
      .create(ByteString.copyFrom(BytesConverter.toBytes("row")))
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("binary")),
        ByteString.copyFrom(Array(1.toByte, 2.toByte, 3.toByte))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("long")),
        ByteString.copyFrom(BytesConverter.toBytes(10000000000L))
      )
      .setCell(
        columnFamily,
        ByteString.copyFrom(BytesConverter.toBytes("string")),
        ByteString.copyFrom(BytesConverter.toBytes("string"))
      )
    batcher.add(mutation)
    batcher.flush()
    batcher.close()

    dataClient.close()
  }
}

case class BigtableRecord(col0: String, col1: Long, col2: String)

object BigtableRecord {
  def apply(i: Int, t: String): BigtableRecord = {
    val s = s"""row${"%03d".format(i)}"""
    BigtableRecord(s, i.toLong, s"String$i: $t")
  }
}

case class AvroBigtableKeyRecord(col0: Array[Byte], col1: Array[Byte])

object AvroBigtableKeyRecord {
  val schemaString =
    s"""{"namespace": "example.avro",
        |   "type": "record",      "name": "User",
        |    "fields": [      {"name": "name", "type": "string"},
        |      {"name": "favorite_number",  "type": ["int", "null"]},
        |        {"name": "favorite_color", "type": ["string", "null"]}      ]    }""".stripMargin

  val avroSchema: Schema = {
    val p = new Schema.Parser
    p.parse(schemaString)
  }

  def apply(i: Int): AvroBigtableKeyRecord = {
    val user = new GenericData.Record(avroSchema);
    user.put("name", s"name${"%03d".format(i)}")
    user.put("favorite_number", i)
    user.put("favorite_color", s"color${"%03d".format(i)}")
    val avroByte = AvroSerdes.serialize(user, avroSchema)
    AvroBigtableKeyRecord(avroByte, avroByte)
  }
}
