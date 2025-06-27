package com.google.cloud.spark.bigtable

import com.google.bigtable.v2.SampleRowKeysResponse
import com.google.cloud.spark.bigtable.datasources.BytesConverter
import com.google.cloud.spark.bigtable.fakeserver.{FakeCustomDataService, FakeServerBuilder}
import com.google.protobuf.ByteString
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CatalogColumnMappingTest
  extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {

  @transient lazy implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CatalogColumnMappingTest")
    .master("local[*]")
    .getOrCreate()

  var fakeCustomDataService: FakeCustomDataService = _
  var emulatorPort: String = _

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def beforeEach(): Unit = {
    fakeCustomDataService = new FakeCustomDataService
    val server = new FakeServerBuilder().addService(fakeCustomDataService).start
    emulatorPort = Integer.toString(server.getPort)
    logInfo("Bigtable mock server started on port " + emulatorPort)
  }

  test("rowkey columns are mapped") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "expected-value",
      "cf1",
      "doesnt matter",
      "some value"
    )

    addSampleKeyResponse("expected-value")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[String]("key") == "expected-value")
  }

  test("non row key columns are mapped") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"},
         |"someCol":{"cf":"cf1", "col":"btcol", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "btcol",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[String]("someCol") == "expected-value")
  }

  test("a single regex match maps correctly") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any").contains("expected-value"))
  }

  test("a static column that also maps a regex will be mapped to both columns") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"},
         |"staticCol":{"cf":"cf1", "col":"any", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any").contains("expected-value"))
    assert(result.first().getAs[String]("staticCol") == "expected-value")
  }

  test("multiple columns mapping the same regex will be added to the regex") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "another",
      "another-expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any").contains("expected-value"))
    assert(result.first().getAs[Map[String, String]]("someCol").get("another").contains("another-expected-value"))
  }

  test("single column mapping to multiple regexes will be added to all") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":".*", "type":"string"},
         |"anotherCol":{"cf":"cf1", "pattern":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any").contains("expected-value"))
    assert(result.first().getAs[Map[String, String]]("anotherCol").get("any").contains("expected-value"))
  }

  test("non matching column will not be added to regex column") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":"^a.*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "not-this-one",
      "whatever"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any").contains("expected-value"))
    assert(!result.first().getAs[Map[String, String]]("someCol").contains("not-this-one"))
  }

  test("matching column qualifier but in different column family will not be added to regex column") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf2",
      "any2",
      "whatever"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any").contains("expected-value"))
    assert(!result.first().getAs[Map[String, String]]("someCol").contains("any2"))
  }

  test("if no bt column matches the regex we should have an empty map") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":"^a.*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "b",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").isEmpty)
  }


  test("catalog with duplicated column name will throw an error") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"},
         |"repeated-col":{"cf":"cf1", "col":"any", "type":"string"}
         |},
         |"regexColumns":{
         |"repeated-col":{"cf":"cf2", "pattern":".*", "type":"string"}
         |}
         |}""".stripMargin

    assertThrows[AnalysisException] {
      spark
        .read
        .format("bigtable")
        .options(createParametersMap(catalog))
        .load()
    }
  }

  test("use re2 for regex matching - \\v doesn't match new line") {
    // Bigtable's backend uses re2 for regex: https://github.com/google/re2
    // To test we are using re2 we exercise some of the intentional differences
    // from re2 syntax to other regex flavors listed at
    // https://swtch.com/~rsc/regexp/regexp3.html#caveats as of May 9 2025
    // Note that we are escaping the pattern twice: We want the json to look
    // like: `"pattern": "a\\va"` since json cannot have unescaped control
    // characters in strings, so we use `a\\\\va' with 2 escapes: `a{\\}{\\}va`
    // which will result in `a\\va`, so that the control character `\v` is
    // escaped at the json string
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":"a\\\\va", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      s"a${0x000A.toChar}a", // Unicode for new line, should not match
      "non-expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      s"a${0x000B.toChar}a", // Unicode for vertical tab, should match
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").keys.count(_ => true) == 1)
    assert(result.first().getAs[Map[String, String]]("someCol").get(s"a${0x000B.toChar}a").contains("expected-value"))
  }

  test("use re2 for regex matching - \\X is not supported") {
    // Bigtable's backend uses re2 for regex: https://github.com/google/re2
    // To test we are using re2 we exercise some of the intentional differences
    // from re2 syntax to other regex flavors listed at
    // https://swtch.com/~rsc/regexp/regexp3.html#caveats as of May 9 2025
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "pattern":"\\X", "type":"string"}
         |}
         |}""".stripMargin


    // Add some data just to have something to try to match
    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "something",
      "it doesn't matter"
    )

    addSampleKeyResponse("some-row")

    // Validate the right exception when using RE2
    assertThrows[SparkException] {
      val result = spark
        .read
        .format("bigtable")
        .options(createParametersMap(catalog))
        .load()
        .collect()
    }
  }

  def createParametersMap(catalog: String): Map[String, String] = {
    Map(
      "catalog" -> catalog,
      "spark.bigtable.project.id" -> "fake-project-id",
      "spark.bigtable.instance.id" -> "fake-instance-id",
      "spark.bigtable.emulator.port" -> emulatorPort
    )
  }

  def addSampleKeyResponse(row: String): Unit = {
    fakeCustomDataService.addSampleRowKeyResponse(
      SampleRowKeysResponse
        .newBuilder()
        .setRowKey(ByteString.copyFrom(BytesConverter.toBytes(row)))
        .build())
  }
}
