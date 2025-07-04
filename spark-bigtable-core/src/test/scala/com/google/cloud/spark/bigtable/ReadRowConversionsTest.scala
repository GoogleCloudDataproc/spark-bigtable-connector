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

  test("buildSimpleRow") {
    var sqlContext: SQLContext = null

    val catalog = s"""{
                     |"table":{"name":"tableName"},
                     |"rowkey":"stringCol",
                     |"columns":{
                     |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"},
                     |"s_col1":{"cf":"cf1", "col":"c1", "type":"int"},
                     |"s_col2":{"cf":"cf1", "col":"c2", "type":"string"},
                     |"s_col3":{"cf":"cf1", "col":"c3", "type":"long"},
                     |"s_col4":{"cf":"cf2", "col":"c4", "type":"double"},
                     |"s_col5":{"cf":"cf3", "col":"c5", "type":"byte"},
                     |"s_col6":{"cf":"cf3", "col":"c6", "type":"binary"}
                     |}
                     |}""".stripMargin

    val relation =
      BigtableRelation(createParametersMap(catalog), None)(sqlContext)

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
      "stringCol",
      "s_col1",
      "s_col2",
      "s_col3",
      "s_col4",
      "s_col5",
      "s_col6"
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)

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

    val catalog = s"""{
                     |"table":{"name":"tableName"},
                     |"rowkey":"stringCol",
                     |"columns":{
                     |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"},
                     |"s_col1":{"cf":"cf1", "col":"c1", "type":"string"}
                     |}
                     |}""".stripMargin

    val relation =
      BigtableRelation(createParametersMap(catalog), None)(sqlContext)

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
      "s_col1",
      "stringCol"
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)

    assert(actualSparkRow.getAs[String](0) == "correctValue")
    assert(actualSparkRow.getAs[String](1) == "fooRowKey")
  }

  test("buildRowNonexistent") {
    var sqlContext: SQLContext = null

    val catalog: String =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"stringCol",
         |"columns":{
         |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"}
         |}
         |}""".stripMargin

    val relation =
      BigtableRelation(createParametersMap(catalog), None)(sqlContext)

    // We just use a simple row key value since that logic is covered in parseRowKey tests
    val rowkey: ByteString =
      ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
    val cells = List[RowCell](
      createRowCell("cf1", "c1", 100000, BytesConverter.toBytes("otherColumn"))
    )
    val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

    val fields = Seq(
      "s_col1",
      "stringCol",
      "s_col1"
    )
    val actualSparkRow: SparkRow =
      ReadRowConversions.buildRow(fields, bigtableRow, relation.catalog)

    assert(actualSparkRow.get(0) == null)
    assert(actualSparkRow.getAs[String](1) == "fooRowKey")
    assert(actualSparkRow.get(2) == null)
  }

  test("buildRowWithMismatchingTypeLengths") {
    var sqlContext: SQLContext = null

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

        val catalog = s"""{
                         |"table":{"name":"tableName"},
                         |"rowkey":"stringCol",
                         |"columns":{
                         |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"},
                         |"s_col1":{"cf":"cf1", "col":"c1", "type":"$colType", "length":$colLength}
                         |}
                         |}""".stripMargin

        val relation =
          BigtableRelation(createParametersMap(catalog), None)(sqlContext)

        // We just use a simple row key value since that logic is covered in parseRowKey tests
        val rowkey: ByteString =
          ByteString.copyFrom(BytesConverter.toBytes("fooRowKey"))
        val cells = List[RowCell](
          createRowCell("cf1", "c1", 0, rowCellBytes)
        )
        val bigtableRow: BigtableRow = createBigtableRow(rowkey, cells)

        val fields = Seq(
          "stringCol",
          "s_col1"
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
