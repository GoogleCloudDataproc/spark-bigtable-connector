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

import com.google.cloud.bigtable.data.v2.models.{RowCell, Row => BigtableRow}
import com.google.cloud.spark.bigtable.datasources._
import com.google.protobuf.ByteString
import org.apache.spark.sql.{SQLContext, Row => SparkRow}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.JavaConverters._

class ReadRowConversionsTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {
  // The functions tested in this module only use the catalog to determine whether the row key
  // is compound or not, since other details (column names, etc.) are passed to them as arguments
  // directly. Therefore, we only use dummy catalogs to distinguish between these two cases.
  val basicCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol",
       |"columns":{
       |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"}
       |}
       |}""".stripMargin
  val compoundRowKeyCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol:stringCol2",
       |"columns":{
       |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"},
       |"stringCol2":{"cf":"rowkey", "col":"stringCol2", "type":"string"}
       |}
       |}""".stripMargin

  test("buildSimpleRow") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    // We just use a simple row key value since that logic is covered in parseRowKey tests
    val rowkey: ByteString =
      ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
    val cells = List[RowCell](
      createRowCell("cf1", "c1", 0, BytesConverter.toBytes(678)),
      createRowCell("cf1", "c2", 1000000, BytesConverter.toBytes("colVal$12")),
      createRowCell("cf1", "c3", 0, BytesConverter.toBytes(9898989898989898L)),
      createRowCell("cf2", "c4", 100, BytesConverter.toBytes(-333.444)),
      createRowCell("cf3", "c5", 999999, Array[Byte](15.toByte)),
      createRowCell("cf3", "c6", 0, Array[Byte](100, 110, -120))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    // For row key field we have to use the same name as the one in basicCatalog
    // since buildRow gets these columns from catalog.getRowKeyColumns
    val fields = Seq(
      Field("stringCol", "rowkey", "stringCol", Option("string")),
      Field("s_col1", "cf1", "c1", Option("int")),
      Field("s_col2", "cf1", "c2", Option("string")),
      Field("s_col3", "cf1", "c3", Option("long")),
      Field("s_col4", "cf2", "c4", Option("double")),
      Field("s_col5", "cf3", "c5", Option("byte")),
      Field("s_col6", "cf3", "c6", Option("binary"))
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog).get

    assert(actualSparkRow.getAs[String](0) == "fooRowKey")
    assert(actualSparkRow.getAs[Int](1) == 678)
    assert(actualSparkRow.getAs[String](2) == "colVal$12")
    assert(actualSparkRow.getAs[Long](3) == 9898989898989898L)
    assert(actualSparkRow.getAs[Double](4) == -333.444)
    assert(actualSparkRow.getAs[Byte](5) == 15.toByte)
    assert(
      actualSparkRow
        .get(6)
        .asInstanceOf[Array[Byte]]
        .sameElements(Array[Byte](100, 110, -120))
    )
  }

  test("buildRowAdditionalTimestampsAndColumns") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    // We just use a simple row key value since that logic is covered in parseRowKey tests
    val rowkey: ByteString =
      ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
    // Based on RowCell's compareByNative(), cells
    // are ordered by reverse chronological order.
    val cells = List[RowCell](
      createRowCell("cf0", "c1", 100000, BytesConverter.toBytes("otherCf1")),
      createRowCell("cf1", "c0", 100000, BytesConverter.toBytes("otherValue1")),
      createRowCell(
        "cf1",
        "c1",
        100000,
        BytesConverter.toBytes("correctValue")
      ),
      createRowCell("cf1", "c1", 10000, BytesConverter.toBytes("oldValue")),
      createRowCell("cf1", "c2", 100000, BytesConverter.toBytes("otherValue2")),
      createRowCell("cf2", "c2", 100000, BytesConverter.toBytes("otherCf2"))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    val fields = Seq(
      Field("s_col1", "cf1", "c1", Option("string")),
      Field("stringCol", "rowkey", "stringCol", Option("string"))
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog).get

    assert(actualSparkRow.getAs[String](0) == "correctValue")
    assert(actualSparkRow.getAs[String](1) == "fooRowKey")
  }

  test("buildRowNonexistent") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    // We just use a simple row key value since that logic is covered in parseRowKey tests
    val rowkey: ByteString =
      ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
    val cells = List[RowCell](
      createRowCell("cf1", "c1", 100000, BytesConverter.toBytes("otherColumn"))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    val fields = Seq(
      Field("s_col1", "cf1", "nonexistentColumn", Option("string")),
      Field("stringCol", "rowkey", "stringCol", Option("string")),
      Field("s_col1", "nonexistentCf", "c1", Option("string"))
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog).get

    assert(actualSparkRow.get(0) == null)
    assert(actualSparkRow.getAs[String](1) == "fooRowKey")
    assert(actualSparkRow.get(2) == null)
  }

  test("buildRowWithMismatchingTypeLengths") {
    var sqlContext: SQLContext = null
    // The parseRowKey method does not use the catalog, just use a dummy one.
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val testData = Table(
      ("rowCellBytes", "colType", "colLength"),
      (
        Array[Byte](10.toByte, 20.toByte, -30.toByte, 40.toByte, -50.toByte),
        "int",
        4
      ),
      (Array[Byte](10.toByte, 20.toByte, -30.toByte), "int", 4),
      (
        Array[Byte](
          1.toByte,
          2.toByte,
          3.toByte,
          -4.toByte,
          5.toByte,
          6.toByte,
          7.toByte,
          8.toByte,
          9.toByte
        ),
        "double",
        8
      ),
      (
        Array[Byte](-1.toByte, 2.toByte, 3.toByte, -4.toByte, 5.toByte),
        "float",
        4
      ),
      (Array[Byte](0.toByte, 0.toByte, 0.toByte, 0.toByte), "string", 3)
    )
    forAll(testData) {
      (rowCellBytes: Array[Byte], colType: String, colLength: Int) =>
        var sqlContext: SQLContext = null
        val relation =
          BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

        // We just use a simple row key value since that logic is covered in parseRowKey tests
        val rowkey: ByteString =
          ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
        val cells = List[RowCell](
          createRowCell("cf1", "c1", 0, rowCellBytes)
        )
        val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

        val fields = Seq(
          Field("stringCol", "rowkey", "stringCol", Option("string")),
          Field("s_col1", "cf1", "c1", Option(colType), len = colLength)
        )
        intercept[IllegalArgumentException] {
          ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)
        }
    }
  }

  test("buildRowWithRegexQualifier") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"stringCol",
         |"columns":{
         |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"}
         |},
         |"regexColumns": {
         |  "s_col1": {
         |    "cf": "cf1",
         |    "pattern": "(?i)attr\\\\d+",
         |    "type": "string"
         |  }
         |}
         |}""".stripMargin
    val params = createParametersMap(catalog)
    var sqlContext: SQLContext = null
    val relation = BigtableRelation(params, None)(sqlContext)

    val testRowsMap = Map(
      "user1" -> createBigtableRow(
        ByteString.copyFrom(BytesConverter.toBytes("user1")),
        List(
          createRowCell(
            "cf1",
            "ATTR123",
            0,
            BytesConverter.toBytes("value1")
          ),
          createRowCell(
            "cf1",
            "attr456",
            0,
            BytesConverter.toBytes("value2")
          ),
          createRowCell(
            "cf1",
            "other_col",
            0,
            BytesConverter.toBytes("value3")
          )
        )
      ),
      "user2" -> createBigtableRow(
        ByteString.copyFrom(BytesConverter.toBytes("user2")),
        List(
          createRowCell(
            "cf1",
            "prefix_attr789",
            0,
            BytesConverter.toBytes("value4")
          ),
          createRowCell(
            "cf1",
            "ATTR101_suffix",
            0,
            BytesConverter.toBytes("value5")
          )
        )
      ),
      "user3" -> createBigtableRow(
        ByteString.copyFrom(BytesConverter.toBytes("user3")),
        List(
          createRowCell(
            "cf1",
            "no_match_1",
            0,
            BytesConverter.toBytes("value6")
          ),
          createRowCell(
            "cf1",
            "no_match_2",
            0,
            BytesConverter.toBytes("value7")
          )
        )
      )
    )

    val fields = Seq(
      Field("stringCol", "rowkey", "stringCol", Option("string")),
      Field("s_col1", "cf1", "(?i)attr\\d+", Option("string"))
    )

    val user1SparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, testRowsMap("user1"), relation.catalog).get
    assert(user1SparkRow.getAs[String](0) == "user1")
    val user1Map = user1SparkRow.getMap[String, String](1)
    assert(user1Map.size == 2)
    assert(user1Map("ATTR123") == "value1")
    assert(user1Map("attr456") == "value2")

    // This case would match with find() but not with matches()
    val user2SparkRowOption: Option[SparkRow] =
      ReadRowConversions.buildRow(fields, testRowsMap("user2"), relation.catalog)
    assert(user2SparkRowOption.isEmpty)

    val user3SparkRowOption: Option[SparkRow] =
      ReadRowConversions.buildRow(fields, testRowsMap("user3"), relation.catalog)
    assert(user3SparkRowOption.isEmpty)
  }

  def createParametersMap(catalog: String): Map[String, String] = {
    Map(
      "catalog" -> catalog,
      "spark.bigtable.project.id" -> "fake-project-id",
      "spark.bigtable.instance.id" -> "fake-instance-id",
      "spark.bigtable.write.timestamp.milliseconds" -> "10000"
    )
  }
  def createRowkey(rowkeyBytes: Array[Byte]): ByteString = {
    ByteString.copyFrom(rowkeyBytes)
  }

  // Since cellValue could have the original value of String, Double, etc.,
  //  we get the converted byte array value directly.
  def createRowCell(
      colFamily: String,
      colQualifier: String,
      timestamp: Long,
      cellValue: Array[Byte],
      labels: List[String] = List[String]()
  ): RowCell = {
    RowCell.create(
      colFamily,
      ByteString.copyFrom(BytesConverter.toBytes(colQualifier)),
      timestamp,
      labels.asJava,
      ByteString.copyFrom(cellValue)
    )
  }

  def createBigtableRow(
      rowkey: ByteString,
      rowCells: List[RowCell]
  ): BigtableRow = {
    BigtableRow.create(rowkey, rowCells.asJava)
  }
}
