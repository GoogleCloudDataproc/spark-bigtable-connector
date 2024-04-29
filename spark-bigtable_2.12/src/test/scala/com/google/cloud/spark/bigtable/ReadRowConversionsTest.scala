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

  test("parseSimpleRowkey") {
    var sqlContext: SQLContext = null
    // The parseRowKey method does not use the catalog, just use a dummy one.
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val testData = Table(
      ("rowkeyBytes", "rowkeyType", "expectedValue"),
      (
        Array[Byte](10.toByte, 20.toByte, -30.toByte),
        "binary",
        Array[Byte](10.toByte, 20.toByte, -30.toByte)
      ),
      (BytesConverter.toBytes(9885673210123L), "long", 9885673210123L),
      // Using byte '0' in string columns is allowed in non-compound row keys.
      (
        BytesConverter.toBytes("foo\u0000bar\u0000"),
        "string",
        "foo\u0000bar\u0000"
      )
    )
    forAll(testData) {
      (rowkeyBytes: Array[Byte], rowkeyType: String, expectedValue: Any) =>
        val rowkey: ByteString = createRowkey(rowkeyBytes)
        // Use any dummy cell values
        val cells = List[RowCell](
          createRowCell("col_family", "col_bt", 0, BytesConverter.toBytes(0))
        )
        val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

        val field: Field =
          Field("spark_col", "rowkey", "bt_col", Option(rowkeyType))
        val keyFields = Seq(field)
        val prsedRowkey: Map[Field, Any] = ReadRowConversions.parseRowKey(
          bigtableRow,
          keyFields,
          relation.catalog
        )

        // If the original row key type is Array[Byte], we need to use array equality.
        val actualValue = prsedRowkey.getOrElse(field, "nonexistent_value")
        actualValue match {
          case actual: Array[Byte] =>
            assert(actual.sameElements(expectedValue.asInstanceOf[Array[Byte]]))
          case actual => assert(actual == expectedValue)
        }
    }
  }

  test("parseCompoundRowkeyFixedLenTypes") {
    var sqlContext: SQLContext = null
    // The parseRowKey method does not use the catalog, just use a dummy one.
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val rowKeyValues = Array[Any](100L, 1000000123L, -987123L)
    val rowKeyBytes = Array[Array[Byte]](
      BytesConverter.toBytes(100L),
      BytesConverter.toBytes(1000000123L),
      BytesConverter.toBytes(-987123L)
    )
    val keyFields = Seq(
      Field(
        sparkColName = "s1",
        btColFamily = "rowkey",
        btColName = "bt1",
        simpleType = Option("long"),
        len = -1
      ),
      Field(
        sparkColName = "s2",
        btColFamily = "rowkey",
        btColName = "bt2",
        simpleType = Option("long"),
        len = -1
      ),
      Field(
        sparkColName = "s3",
        btColFamily = "rowkey",
        btColName = "bt3",
        simpleType = Option("long"),
        len = -1
      )
    )

    val rowkey: ByteString = createRowkey(
      rowKeyBytes.foldLeft(Array[Byte]())((rowKey, x) => rowKey ++ x)
    )
    val cells = List[RowCell](
      createRowCell("col_family", "col_bt", 0, BytesConverter.toBytes(0))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    val prsedRowkey: Map[Field, Any] =
      ReadRowConversions.parseRowKey(bigtableRow, keyFields, relation.catalog)

    (keyFields zip rowKeyValues).foreach {
      case (keyField, rowKeyValue) => {
        assert(
          prsedRowkey.getOrElse(keyField, "nonexistent_value") == rowKeyValue
        )
      }
    }
  }

  test("parseCompoundRowkeyString") {
    var sqlContext: SQLContext = null
    // The parseRowKey method does not use the catalog, just use a dummy one.
    val relation =
      BigtableRelation(createParametersMap(compoundRowKeyCatalog), None)(
        sqlContext
      )

    val rowKeyValues =
      Array[String]("fixedLen", "delimiter\u0000", "delim2\u0000", "fixedLen2")
    val rowKeyBytes = rowKeyValues.map(BytesConverter.toBytes)
    println("rewrererw")
    rowKeyBytes.foreach(x => { x.foreach(y => print(y + ",")); println() })
    val keyFields = Seq(
      Field(
        sparkColName = "s1",
        btColFamily = "rowkey",
        btColName = "bt1",
        simpleType = Option("string"),
        len = 8
      ),
      Field(
        sparkColName = "s2",
        btColFamily = "rowkey",
        btColName = "bt2",
        simpleType = Option("string"),
        len = -1
      ),
      Field(
        sparkColName = "s3",
        btColFamily = "rowkey",
        btColName = "bt3",
        simpleType = Option("string"),
        len = -1
      ),
      Field(
        sparkColName = "s4",
        btColFamily = "rowkey",
        btColName = "bt4",
        simpleType = Option("string"),
        len = 9
      )
    )

    val rowkey: ByteString = createRowkey(
      rowKeyBytes.foldLeft(Array[Byte]())((rowKey, x) => rowKey ++ x)
    )
    val cells = List[RowCell](
      createRowCell("col_family", "col_bt", 0, BytesConverter.toBytes(0))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    val prsedRowkey: Map[Field, Any] =
      ReadRowConversions.parseRowKey(bigtableRow, keyFields, relation.catalog)

    (keyFields zip rowKeyValues).foreach {
      case (keyField, rowKeyValue) => {
        prsedRowkey
          .getOrElse(keyField, "nonexistent_value")
          .toString
          .getBytes
          .foreach(x => print(x + ","))
        println()
        assert(
          prsedRowkey.getOrElse(keyField, "nonexistent_value") == rowKeyValue
        )
      }
    }
  }

  test("parseInvalidCompoundRowkeyString") {
    var sqlContext: SQLContext = null
    // The parseRowKey method does not use the catalog, just use a dummy one.
    val relation =
      BigtableRelation(createParametersMap(compoundRowKeyCatalog), None)(
        sqlContext
      )

    val rowKeyValues = Array[String]("fixedLen", "noDelimiter")
    val rowKeyBytes = rowKeyValues.map(BytesConverter.toBytes)
    val keyFields = Seq(
      Field(
        sparkColName = "s1",
        btColFamily = "rowkey",
        btColName = "bt1",
        simpleType = Option("string"),
        len = 8
      ),
      Field(
        sparkColName = "s2",
        btColFamily = "rowkey",
        btColName = "bt2",
        simpleType = Option("string"),
        len = -1
      )
    )

    val rowkey: ByteString = createRowkey(
      rowKeyBytes.foldLeft(Array[Byte]())((rowKey, x) => rowKey ++ x)
    )
    val cells = List[RowCell](
      createRowCell("col_family", "col_bt", 0, BytesConverter.toBytes(0))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    intercept[IllegalArgumentException] {
      val prsedRowkey: Map[Field, Any] =
        ReadRowConversions.parseRowKey(bigtableRow, keyFields, relation.catalog)
    }
  }

  test("parseCompoundRowkeyBinary") {
    var sqlContext: SQLContext = null
    // The parseRowKey method does not use the catalog, just use a dummy one.
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val rowKeyValues =
      Array[Array[Byte]](Array[Byte](5, 6, 7), Array[Byte](-2, -3, -4, -5, -6))
    val rowKeyBytes = rowKeyValues
    val keyFields = Seq(
      Field(
        sparkColName = "s1",
        btColFamily = "rowkey",
        btColName = "bt1",
        simpleType = Option("binary"),
        len = 3
      ),
      Field(
        sparkColName = "s2",
        btColFamily = "rowkey",
        btColName = "bt2",
        simpleType = Option("binary"),
        len = -1
      )
    )

    val rowkey: ByteString = createRowkey(
      rowKeyBytes.foldLeft(Array[Byte]())((rowKey, x) => rowKey ++ x)
    )
    val cells = List[RowCell](
      createRowCell("col_family", "col_bt", 0, BytesConverter.toBytes(0))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    val prsedRowkey: Map[Field, Any] =
      ReadRowConversions.parseRowKey(bigtableRow, keyFields, relation.catalog)

    (keyFields zip rowKeyValues).foreach {
      case (keyField, rowKeyValue) => {
        assert(
          prsedRowkey
            .getOrElse(keyField, "nonexistent_value")
            .asInstanceOf[Array[Byte]]
            .sameElements(rowKeyValue)
        )
      }
    }
  }

  test("buildSimpleRow") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    // We just use a simple row key value since that logic is covered in parseRowKey tests
    val rowkey: ByteString =
      ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
    val cells = List[RowCell](
      createRowCell("cf1", "c1", 1000000, BytesConverter.toBytes("colVal$12")),
      createRowCell("cf1", "c2", 0, BytesConverter.toBytes(9898989898989898L)),
      createRowCell("cf2", "c3", 0, Array[Byte](100, 110, -120))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    // For row key field we have to use the same name as the one in basicCatalog
    // since buildRow gets these columns from catalog.getRowKeyColumns
    val fields = Seq(
      Field("stringCol", "rowkey", "stringCol", Option("string")),
      Field("s_col1", "cf1", "c1", Option("string")),
      Field("s_col2", "cf1", "c2", Option("long")),
      Field("s_col3", "cf2", "c3", Option("binary"))
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)

    assert(actualSparkRow.getAs[String](0) == "fooRowKey")
    assert(actualSparkRow.getAs[String](1) == "colVal$12")
    assert(actualSparkRow.getAs[Long](2) == 9898989898989898L)
    assert(
      actualSparkRow
        .get(3)
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
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)

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
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)

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
        "long",
        8
      ),
      (
        Array[Byte](
          1.toByte,
          2.toByte,
          3.toByte,
          -4.toByte,
          5.toByte,
          6.toByte,
          7.toByte
        ),
        "long",
        8
      )
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
