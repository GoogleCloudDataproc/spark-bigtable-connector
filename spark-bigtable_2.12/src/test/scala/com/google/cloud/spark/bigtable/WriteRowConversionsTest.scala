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

import com.google.bigtable.v2.{MutateRowsRequest, Mutation}
import com.google.cloud.bigtable.data.v2.models.{RowCell, Row => BigtableRow}
import com.google.cloud.spark.bigtable.datasources._
import com.google.protobuf.ByteString
import org.apache.spark.sql.{SQLContext, Row => SparkRow}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.collection.JavaConverters._

class WriteRowConversionsTest
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

  test("convertToRowMutationSimpleRowKey") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val testData = Table(
      ("sparkRow", "byteValues", "dataType"),
      // String row key and column
      (
        SparkRow("foo\n\u0000bar12$%", "fooRow"),
        Seq(
          BytesConverter.toBytes("foo\n\u0000bar12$%"),
          BytesConverter.toBytes("fooRow")
        ),
        "string"
      ),
      // Long row key and column
      (
        SparkRow(1248987L, -987123L),
        Seq(BytesConverter.toBytes(1248987L), BytesConverter.toBytes(-987123L)),
        "long"
      )
    )

    forAll(testData) {
      (sparkRow: SparkRow, byteValues: Seq[Array[Byte]], dataType: String) =>
        val writeRowConversions = new WriteRowConversions(
          relation.catalog,
          relation.schema,
          relation.writeTimestampMicros
        )
        writeRowConversions.rowKeyIndexAndFields = Seq(
          (0, Field("spark_rowkey", "rowkey", "bt_rowkey", Option(dataType)))
        )
        writeRowConversions.columnIndexAndField =
          Array((1, Field("col1", "cf1", "bt_col1", Option(dataType))))

        val actualEntry: MutateRowsRequest.Entry =
          writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()
        val expectedEntry: MutateRowsRequest.Entry =
          MutateRowsRequest.Entry
            .newBuilder()
            .setRowKey(ByteString.copyFrom(byteValues(0)))
            .addMutations(
              Mutation
                .newBuilder()
                .setSetCell(
                  Mutation.SetCell
                    .newBuilder()
                    .setFamilyName("cf1")
                    .setColumnQualifier(
                      ByteString.copyFrom(BytesConverter.toBytes("bt_col1"))
                    )
                    .setTimestampMicros(10000000L)
                    .setValue(ByteString.copyFrom(byteValues(1)))
                )
            )
            .build()
        assert(actualEntry == expectedEntry)
    }
  }

  test("convertToRowMutationCompoundRowKey") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(compoundRowKeyCatalog), None)(
        sqlContext
      )

    val sparkRow =
      SparkRow("rowkey2\u0000", 111L, "colValue", "fixedLen", -444L)

    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )
    // spark_rowkey# is specified for creating a dummy Field object. However, since we pass the
    // index as the first element of rkIndexAndFields, it's not actually used in the code logic.
    writeRowConversions.rowKeyIndexAndFields = Seq(
      (
        1,
        Field("spark_rowkey0", "rowkey", "bt_rowkey", Option("long"), len = -1)
      ),
      (
        0,
        Field(
          "spark_rowkey2",
          "rowkey",
          "bt_rowkey",
          Option("string"),
          len = -1
        )
      ),
      (
        3,
        Field("spark_rowkey1", "rowkey", "bt_rowkey", Option("string"), len = 8)
      ),
      (
        4,
        Field("spark_rowkey3", "rowkey", "bt_rowkey", Option("long"), len = -1)
      )
    )
    writeRowConversions.columnIndexAndField =
      Array((2, Field("col1", "cf1", "bt_col1", Option("string"))))

    val actualEntry: MutateRowsRequest.Entry =
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()
    val expectedRowKey = (
      BytesConverter.toBytes(111L)
        ++ BytesConverter.toBytes("rowkey2\u0000")
        ++ BytesConverter.toBytes("fixedLen")
        ++ BytesConverter.toBytes(-444L)
    )
    val expectedEntry: MutateRowsRequest.Entry =
      MutateRowsRequest.Entry
        .newBuilder()
        .setRowKey(ByteString.copyFrom(expectedRowKey))
        .addMutations(
          Mutation
            .newBuilder()
            .setSetCell(
              Mutation.SetCell
                .newBuilder()
                .setFamilyName("cf1")
                .setColumnQualifier(
                  ByteString.copyFrom(BytesConverter.toBytes("bt_col1"))
                )
                .setTimestampMicros(10000000L)
                .setValue(
                  ByteString.copyFrom(BytesConverter.toBytes("colValue"))
                )
            )
        )
        .build()
    assert(actualEntry == expectedEntry)
  }

  test("convertToRowMutationIllegalCompoundRowKey") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(compoundRowKeyCatalog), None)(
        sqlContext
      )

    // All string columns in a compound row key should either have a fixed length
    // or end with byte '0'.
    val sparkRow = SparkRow("rowkey2", 111L, "colValue", "ROWKEY3", -777L)
    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )
    writeRowConversions.rowKeyIndexAndFields = Seq(
      (
        1,
        Field("spark_rowkey0", "rowkey", "bt_rowkey", Option("long"), len = -1)
      ),
      (
        0,
        Field(
          "spark_rowkey2",
          "rowkey",
          "bt_rowkey",
          Option("string"),
          len = -1
        )
      ),
      (
        3,
        Field(
          "spark_rowkey1",
          "rowkey",
          "bt_rowkey",
          Option("string"),
          len = -1
        )
      ),
      (
        4,
        Field("spark_rowkey3", "rowkey", "bt_rowkey", Option("long"), len = -1)
      )
    )
    writeRowConversions.columnIndexAndField =
      Array((2, Field("col1", "cf1", "bt_col1", Option("string"))))

    intercept[IllegalArgumentException] {
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()
    }
  }

  test("convertToRowMutationMultipleColumns") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val sparkRow = SparkRow(
      "StringCOL_123",
      -1999L,
      6767676767L,
      "foo\u3943RowKEY",
      45698701L
    )

    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )
    writeRowConversions.rowKeyIndexAndFields =
      Seq((3, Field("spark_rowkey", "rowkey", "bt_rowkey", Option("string"))))
    writeRowConversions.columnIndexAndField = Array(
      (2, Field("col2", "cf1", "bt_col2", Option("long"))),
      (4, Field("col4", "cf2", "bt_col4", Option("long"))),
      (0, Field("col0", "cf1", "bt_col0", Option("string"))),
      (1, Field("col1", "cf3", "bt_col1", Option("long")))
    )

    val actualEntry: MutateRowsRequest.Entry =
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()

    val expectedEntry: MutateRowsRequest.Entry =
      MutateRowsRequest.Entry
        .newBuilder()
        .setRowKey(
          ByteString.copyFrom(BytesConverter.toBytes("foo\u3943RowKEY"))
        )
        .addMutations(
          Mutation
            .newBuilder()
            .setSetCell(
              Mutation.SetCell
                .newBuilder()
                .setFamilyName("cf1")
                .setColumnQualifier(
                  ByteString.copyFrom(BytesConverter.toBytes("bt_col2"))
                )
                .setTimestampMicros(10000000L)
                .setValue(
                  ByteString.copyFrom(BytesConverter.toBytes(6767676767L))
                )
            )
        )
        .addMutations(
          Mutation
            .newBuilder()
            .setSetCell(
              Mutation.SetCell
                .newBuilder()
                .setFamilyName("cf2")
                .setColumnQualifier(
                  ByteString.copyFrom(BytesConverter.toBytes("bt_col4"))
                )
                .setTimestampMicros(10000000L)
                .setValue(
                  ByteString.copyFrom(BytesConverter.toBytes(45698701L))
                )
            )
        )
        .addMutations(
          Mutation
            .newBuilder()
            .setSetCell(
              Mutation.SetCell
                .newBuilder()
                .setFamilyName("cf1")
                .setColumnQualifier(
                  ByteString.copyFrom(BytesConverter.toBytes("bt_col0"))
                )
                .setTimestampMicros(10000000L)
                .setValue(
                  ByteString.copyFrom(BytesConverter.toBytes("StringCOL_123"))
                )
            )
        )
        .addMutations(
          Mutation
            .newBuilder()
            .setSetCell(
              Mutation.SetCell
                .newBuilder()
                .setFamilyName("cf3")
                .setColumnQualifier(
                  ByteString.copyFrom(BytesConverter.toBytes("bt_col1"))
                )
                .setTimestampMicros(10000000L)
                .setValue(ByteString.copyFrom(BytesConverter.toBytes(-1999L)))
            )
        )
        .build()
    assert(actualEntry == expectedEntry)
  }

  test("convertToRowMutationNoTimestamp") {
    val beforeTimeMicros = System.currentTimeMillis() * 1000

    var sqlContext: SQLContext = null
    val parametersMapWithoutTimestamp =
      Map(
        "catalog" -> basicCatalog,
        "spark.bigtable.project.id" -> "fake-project-id",
        "spark.bigtable.instance.id" -> "fake-instance-id"
      )
    val relation =
      BigtableRelation(parametersMapWithoutTimestamp, None)(sqlContext)

    val sparkRow = SparkRow("rowkeyfoo", "colfoo")

    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )
    writeRowConversions.rowKeyIndexAndFields =
      Seq((0, Field("spark_rowkey", "rowkey", "bt_rowkey", Option("string"))))
    writeRowConversions.columnIndexAndField =
      Array((1, Field("col1", "cf1", "bt_col1", Option("string"))))

    val actualEntry: MutateRowsRequest.Entry =
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()

    val afterTimeMicros = System.currentTimeMillis() * 1000
    val actualTimestamp =
      actualEntry.getMutations(0).getSetCell().getTimestampMicros()

    assert(actualTimestamp >= beforeTimeMicros)
    assert(actualTimestamp <= afterTimeMicros)
  }

  // There should only be exception when using incompatible
  // types, e.g., converting a 5-byte array to int.
  test("convertToRowMutationMismatchedTypes") {
    var sqlContext: SQLContext = null
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)

    val testData = Table(
      ("sparkRow", "dataType", "shouldFail"),
      (SparkRow(123, "fooCol"), "string", false),
      (SparkRow(235.35f, 2354.00897), "long", false),
      (SparkRow("324", "fooCol"), "long", true),
      (SparkRow(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9), 678), "long", true),
      (SparkRow(true, 678), "long", true)
    )

    forAll(testData) {
      (sparkRow: SparkRow, dataType: String, shouldFail: Boolean) =>
        val writeRowConversions = new WriteRowConversions(
          relation.catalog,
          relation.schema,
          relation.writeTimestampMicros
        )
        writeRowConversions.rowKeyIndexAndFields = Seq(
          (0, Field("spark_rowkey", "rowkey", "bt_rowkey", Option(dataType)))
        )
        writeRowConversions.columnIndexAndField =
          Array((1, Field("col1", "cf1", "bt_col1", Option(dataType))))

        if (shouldFail) {
          intercept[Exception] {
            writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()
          }
        } else {
          writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto()
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
