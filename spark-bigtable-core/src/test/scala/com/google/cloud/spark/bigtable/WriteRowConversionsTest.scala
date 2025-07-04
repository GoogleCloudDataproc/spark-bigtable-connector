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
import com.google.cloud.spark.bigtable.datasources._
import com.google.protobuf.ByteString
import org.apache.spark.sql.{SQLContext, Row => SparkRow}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class WriteRowConversionsTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {

  test("convertToRowMutationSimpleRowKey") {
    var sqlContext: SQLContext = null

    val testData = Table(
      ("rowKeyValue", "rowKeyBytes", "mutationCellValue", "mutationCellBytes", "dataType"),
      // String row key and column
      (
        "fooRow",
        BytesConverter.toBytes("fooRow"),
        "foo\n\u0000bar12$%",
        BytesConverter.toBytes("foo\n\u0000bar12$%"),
        "string"
      ),
      // Int row key and column
      (
        1248987,
        BytesConverter.toBytes(1248987),
        -987123,
        BytesConverter.toBytes(-987123),
        "int"
      ),
      // Float row key and column
      (
        -876.2344f,
        BytesConverter.toBytes(-876.2344f),
        1111.00002f,
        BytesConverter.toBytes(1111.00002f),
        "float"
      ),
      // Double row key and column
      (
        46245.43543,
        BytesConverter.toBytes(46245.43543),
        -32222.000024,
        BytesConverter.toBytes(-32222.000024),
        "double"
      ),
      // Long row key and column
      (
        1248987L,
        BytesConverter.toBytes(1248987L),
        -987123L,
        BytesConverter.toBytes(-987123L),
        "long"
      )
    )

    forAll(testData) {
      (rowKeyValue: Any,
       rowKeyBytes: Array[Byte],
       cellMutationValue: Any,
       cellMutationBytes: Array[Byte],
       dataType: String) =>
        val catalog =
          s"""{
             |"table":{"name":"tableName"},
             |"rowkey":"bt_rowkey",
             |"columns":{
             |"spark_rowkey":{"cf":"rowkey", "col":"bt_rowkey", "type":"$dataType"},
             |"col1":{"cf":"cf1", "col":"bt_col1", "type":"$dataType"}
             |}
             |}""".stripMargin.stripMargin

        val relation =
          BigtableRelation(createParametersMap(catalog), None)(sqlContext)

        val sparkRow = SparkRow(
          relation.schema.fields
            .flatMap {
              case f@_ if f.name == "spark_rowkey" => Some(rowKeyValue)
              case f@_ if f.name == "col1" => Some(cellMutationValue)
              case _ => None
            }:_*)

        val writeRowConversions = new WriteRowConversions(
          relation.catalog,
          relation.schema,
          relation.writeTimestampMicros
        )

        val actualEntry: MutateRowsRequest.Entry =
          writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto
        val expectedEntry: MutateRowsRequest.Entry =
          MutateRowsRequest.Entry
            .newBuilder()
            .setRowKey(ByteString.copyFrom(rowKeyBytes))
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
                    .setValue(ByteString.copyFrom(cellMutationBytes))
                )
            )
            .build()
        assert(actualEntry == expectedEntry)
    }
  }

  test("convertToRowMutationCompoundRowKey") {
    var sqlContext: SQLContext = null

    val catalog = s"""{
                     |"table":{"name":"tableName"},
                     |"rowkey":"bt_rowkey0:bt_rowkey1:bt_rowkey2:bt_rowkey3",
                     |"columns":{
                     |"spark_rowkey0":{"cf":"rowkey", "col":"bt_rowkey0", "type":"long"},
                     |"spark_rowkey1":{"cf":"rowkey", "col":"bt_rowkey1", "type":"string", "length": "8"},
                     |"spark_rowkey2":{"cf":"rowkey", "col":"bt_rowkey2", "type":"string"},
                     |"spark_rowkey3":{"cf":"rowkey", "col":"bt_rowkey3", "type":"float"},
                     |"col1":{"cf":"cf1", "col":"bt_col1", "type":"string"}
                     |}
                     |}""".stripMargin

    val relation =
      BigtableRelation(createParametersMap(catalog), None)(
        sqlContext
      )

    val sparkRow =
      SparkRow(
        relation.schema.fields
          .flatMap {
            case f@_ if f.name == "spark_rowkey0" => Some(111L)
            case f@_ if f.name == "spark_rowkey1" => Some("fixedLen")
            case f@_ if f.name == "spark_rowkey2" => Some("rowkey2\u0000")
            case f@_ if f.name == "spark_rowkey3" => Some(-444.444f)
            case f@_ if f.name == "col1" => Some("colValue")
            case _ => None
          }:_*)

    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )

    val actualEntry: MutateRowsRequest.Entry =
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto
    val expectedRowKey = (
      BytesConverter.toBytes(111L)
        ++ BytesConverter.toBytes("fixedLen")
        ++ BytesConverter.toBytes("rowkey2\u0000")
        ++ BytesConverter.toBytes(-444.444f)
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

    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"bt_rowkey0:bt_rowkey1:bt_rowkey2:bt_rowkey3",
         |"columns":{
         |"spark_rowkey0":{"cf":"rowkey", "col":"bt_rowkey0", "type":"long"},
         |"spark_rowkey1":{"cf":"rowkey", "col":"bt_rowkey1", "type":"string"},
         |"spark_rowkey2":{"cf":"rowkey", "col":"bt_rowkey2", "type":"string"},
         |"spark_rowkey3":{"cf":"rowkey", "col":"bt_rowkey3", "type":"float"},
         |"col1":{"cf":"cf1", "col":"bt_col1", "type":"string"}
         |}
         |}""".stripMargin

    val relation =
      BigtableRelation(createParametersMap(catalog), None)(
        sqlContext
      )

    // All string columns in a compound row key should either have a fixed length
    // or end with byte '0'.
    val sparkRow =
      SparkRow(
        relation.schema.fields
          .flatMap {
            case f@_ if f.name == "spark_rowkey0" => Some(111L)
            case f@_ if f.name == "spark_rowkey1" => Some("ROWKEY1")
            case f@_ if f.name == "spark_rowkey2" => Some("rowkey2")
            case f@_ if f.name == "spark_rowkey3" => Some(-444.444f)
            case f@_ if f.name == "col1" => Some("colValue")
            case _ => None
          }:_*)

    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )

    intercept[IllegalArgumentException] {
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto
    }
  }

  test("convertToRowMutationMultipleColumns") {
    var sqlContext: SQLContext = null

    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"bt_rowkey",
         |"columns":{
         |"spark_rowkey":{"cf":"rowkey", "col":"bt_rowkey", "type":"string"},
         |"col0":{"cf":"cf1", "col":"bt_col0", "type":"string"},
         |"col1":{"cf":"cf3", "col":"bt_col1", "type":"int"},
         |"col2":{"cf":"cf1", "col":"bt_col2", "type":"long"},
         |"col3":{"cf":"cf2", "col":"bt_col3", "type":"double"}
         |}
         |}""".stripMargin

    val relation =
      BigtableRelation(createParametersMap(catalog), None)(sqlContext)

    val sparkRow =
      SparkRow(
        relation.schema.fields
          .flatMap {
            case f@_ if f.name == "spark_rowkey" => Some("foo\u3943RowKEY")
            case f@_ if f.name == "col0" => Some("StringCOL_123")
            case f@_ if f.name == "col1" => Some(-1999)
            case f@_ if f.name == "col2" => Some(6767676767L)
            case f@_ if f.name == "col3" => Some(456.98701)
            case _ => None
          }:_*)

    val writeRowConversions = new WriteRowConversions(
      relation.catalog,
      relation.schema,
      relation.writeTimestampMicros
    )

    val actualEntry: MutateRowsRequest.Entry =
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto

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
                  ByteString.copyFrom(BytesConverter.toBytes("bt_col3"))
                )
                .setTimestampMicros(10000000L)
                .setValue(
                  ByteString.copyFrom(BytesConverter.toBytes(456.98701))
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
                .setValue(ByteString.copyFrom(BytesConverter.toBytes(-1999)))
            )
        )
        .build()

    assert(actualEntry.getRowKey == expectedEntry.getRowKey)
    assert(actualEntry.getMutationsCount == actualEntry.getMutationsCount)
    assert(actualEntry.getMutationsList.containsAll(expectedEntry.getMutationsList))
  }

  test("convertToRowMutationNoTimestamp") {
    val beforeTimeMicros = System.currentTimeMillis() * 1000

    var sqlContext: SQLContext = null

    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"bt_rowkey",
         |"columns":{
         |"spark_rowkey":{"cf":"rowkey", "col":"bt_rowkey", "type":"string"},
         |"col0":{"cf":"cf1", "col":"bt_col1", "type":"string"}
         |}
         |}""".stripMargin

    val parametersMapWithoutTimestamp =
      Map(
        "catalog" -> catalog,
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

    val actualEntry: MutateRowsRequest.Entry =
      writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto

    val afterTimeMicros = System.currentTimeMillis() * 1000
    val actualTimestamp =
      actualEntry.getMutations(0).getSetCell.getTimestampMicros

    assert(actualTimestamp >= beforeTimeMicros)
    assert(actualTimestamp <= afterTimeMicros)
  }

  // There should only be exception when using incompatible
  // types, e.g., converting a 5-byte array to int.
  test("convertToRowMutationMismatchedTypes") {
    var sqlContext: SQLContext = null

    val testData = Table(
      ("sparkRow", "dataType", "shouldFail"),
      (SparkRow(123, "fooCol"), "string", false),
      (SparkRow(235.35, 352.3), "float", false),
      (SparkRow(235.35f, 2354.32f), "double", false),
      (SparkRow(235.35, 235432100L), "double", false),
      (SparkRow(235.35f, 2354.00897), "long", false),
      (SparkRow("324", "fooCol"), "long", true),
      (SparkRow(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9), 678), "long", true),
      (SparkRow(true, 678), "long", true)
    )

    forAll(testData) {
      (sparkRow: SparkRow, dataType: String, shouldFail: Boolean) =>
        val catalog =
          s"""{
             |"table":{"name":"tableName"},
             |"rowkey":"bt_rowkey",
             |"columns":{
             |"spark_rowkey":{"cf":"rowkey", "col":"bt_rowkey", "type":"$dataType"},
             |"col0":{"cf":"cf1", "col":"bt_col1", "type":"$dataType"}
             |}
             |}""".stripMargin

        val relation =
          BigtableRelation(createParametersMap(catalog), None)(sqlContext)

        val writeRowConversions = new WriteRowConversions(
          relation.catalog,
          relation.schema,
          relation.writeTimestampMicros
        )

        if (shouldFail) {
          intercept[Exception] {
            writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto
          }
        } else {
          writeRowConversions.convertToBigtableRowMutation(sparkRow).toProto
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

}
