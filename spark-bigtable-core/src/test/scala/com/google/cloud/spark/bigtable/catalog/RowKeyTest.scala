/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.spark.bigtable.catalog

import com.google.cloud.bigtable.data.v2.models.{RowCell, Row => BigtableRow}
import com.google.cloud.spark.bigtable.datasources.{BytesConverter, Field, Utils}
import com.google.cloud.spark.bigtable.Logging
import com.google.cloud.spark.bigtable.filters.RowKeyWrapper
import com.google.protobuf.ByteString
import com.google.common.collect.{Range, TreeRangeSet}
import org.apache.spark.sql.{Row => SparkRow}
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Or, StringStartsWith}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import scala.collection.JavaConverters.seqAsJavaListConverter

class RowKeyTest extends AnyFunSuite with Logging {
  test("Parse non compound row keys") {
    val testData = Table(
      ("rowkeyBytes", "rowkeyType", "expectedValue"),
      (
        Array[Byte](10.toByte, 20.toByte, -30.toByte),
        "binary",
        Array[Byte](10.toByte, 20.toByte, -30.toByte)
      ),
      (BytesConverter.toBytes(false), "boolean", false),
      (Array[Byte](123.toByte), "byte", 123.toByte),
      (BytesConverter.toBytes(4321.toShort), "short", 4321.toShort),
      (BytesConverter.toBytes(-123523), "int", -123523),
      (BytesConverter.toBytes(9885673210123L), "long", 9885673210123L),
      (BytesConverter.toBytes(23.678012f), "float", 23.678012f),
      (
        BytesConverter.toBytes(-123876.987123000123),
        "double",
        -123876.987123000123
      ),
      // Using byte '0' in string columns is allowed in non-compound row keys.
      (
        BytesConverter.toBytes("foo\u0000bar\u0000"),
        "string",
        "foo\u0000bar\u0000"
      )
    )
    forAll(testData) {
      (rowkeyBytes: Array[Byte], rowkeyType: String, expectedValue: Any) =>
        val rowkey: ByteString = ByteString.copyFrom(rowkeyBytes)

        val bigtableRow: BigtableRow = BigtableRow.create(rowkey, List.empty[RowCell].asJava)

        val rowKeyField =
          Field("spark_col_name", "rowkey", "bt_col", Some(rowkeyType))

        val rowKey = RowKey("bt_col", Seq[Field](rowKeyField))

        val parsedRowkey: Map[Field, Any] = rowKey.parseFieldsFromBtRow(bigtableRow)

        assert(parsedRowkey.size == 1)

        // If the original row key type is Array[Byte], we need to use array equality.
        parsedRowkey.get(rowKeyField) match {
          case Some(byteArrayValue: Array[Byte]) =>
            assert(byteArrayValue.sameElements(expectedValue.asInstanceOf[Array[Byte]]))
          case Some(otherValueType) =>
            assert(otherValueType == expectedValue)
          case _ =>
            fail("Parsing result did not contain field")
        }
    }
  }

  test("Parse compound row keys with all fields of fixed size") {
    val rowKeyValues = Array[Any](100, 10.4f, -987123L)

    val rowKeyByteValues = Array[Array[Byte]](
      BytesConverter.toBytes(100),
      BytesConverter.toBytes(10.4f),
      BytesConverter.toBytes(-987123L)
    )

    val keyFields = Seq(
      Field(
        sparkColName = "s1",
        btColFamily = "rowkey",
        btColName = "bt1",
        simpleType = Option("int")
      ),
      Field(
        sparkColName = "s2",
        btColFamily = "rowkey",
        btColName = "bt2",
        simpleType = Option("float")
      ),
      Field(
        sparkColName = "s3",
        btColFamily = "rowkey",
        btColName = "bt3",
        simpleType = Option("long")
      )
    )

    val expectedFieldToValues = (keyFields zip rowKeyValues).toMap

    val rowkeyBytes: ByteString = ByteString.copyFrom(rowKeyByteValues.flatMap(_.toList))

    val bigtableRow: BigtableRow = BigtableRow.create(rowkeyBytes, List.empty[RowCell].asJava)

    val rowKey = RowKey("bt1:bt2:bt3", keyFields)

    val parsedRowkey: Map[Field, Any] = rowKey.parseFieldsFromBtRow(bigtableRow)

    assertResult(expectedFieldToValues)(parsedRowkey)
  }

  test("Parse compound row keys with variable sized strings") {
    val rowKeyValues =
      Array[String]("fixedLen", "delimiter\u0000", "fixedLen2", "delim2\u0000")
    val rowKeyByteValues = rowKeyValues.map(BytesConverter.toBytes)

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
        len = 9
      ),
      Field(
        sparkColName = "s4",
        btColFamily = "rowkey",
        btColName = "bt4",
        simpleType = Option("string"),
        len = -1
      )
    )

    val expectedFieldsToValue = (keyFields zip rowKeyValues).toMap

    val rowkeyBytes: ByteString = ByteString.copyFrom(rowKeyByteValues.flatMap(_.toList))

    val bigtableRow: BigtableRow = BigtableRow.create(rowkeyBytes, List.empty[RowCell].asJava)

    val rowKey = RowKey("bt1:bt2:bt3:bt4", keyFields)

    val parsedRowkey: Map[Field, Any] = rowKey.parseFieldsFromBtRow(bigtableRow)

    assertResult(expectedFieldsToValue)(parsedRowkey)
  }

  test("Compound keys with variable sized strings should throw an error if the string does not end on a delimiter") {
    val rowKeyValues = Array[String]("fixedLen", "noDelimiter")
    val rowKeyByteValues = rowKeyValues.map(BytesConverter.toBytes)
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

    val rowkeyBytes: ByteString = ByteString.copyFrom(rowKeyByteValues.flatMap(_.toList))

    val bigtableRow: BigtableRow = BigtableRow.create(rowkeyBytes, List.empty[RowCell].asJava)

    val rowKey = RowKey("bt1:bt2", keyFields)

    intercept[IllegalArgumentException] {
      rowKey.parseFieldsFromBtRow(bigtableRow)
    }
  }

  test("Compound keys with variable sized strings should throw an error if the string has a delimiter not at the end of the string") {
    val rowKeyValues = Array[String]("fixedLen", "delim\u0000iter\u0000")
    val rowKeyByteValues = rowKeyValues.map(BytesConverter.toBytes)
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

    val rowkeyBytes: ByteString = ByteString.copyFrom(rowKeyByteValues.flatMap(_.toList))

    val bigtableRow: BigtableRow = BigtableRow.create(rowkeyBytes, List.empty[RowCell].asJava)

    val rowKey = RowKey("bt1:bt2", keyFields)

    intercept[IllegalArgumentException] {
      rowKey.parseFieldsFromBtRow(bigtableRow)
    }
  }

  test("Compound row keys with fixed size byte fields or variable sized as the last field are parsed") {
    val rowKeyValues =
      Array[Array[Byte]](Array[Byte](5, 6, 7), Array[Byte](-2, -3, -4, -5, -6))
    val rowKeyByteValues = rowKeyValues
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

    val expectedFieldsToValue = (keyFields zip rowKeyValues).toMap

    val rowkeyBytes: ByteString = ByteString.copyFrom(rowKeyByteValues.flatMap(_.toList))

    val bigtableRow: BigtableRow = BigtableRow.create(rowkeyBytes, List.empty[RowCell].asJava)

    val rowKey = RowKey("bt1:bt2", keyFields)

    val parsedRowkey: Map[Field, Any] = rowKey.parseFieldsFromBtRow(bigtableRow)

    assert(parsedRowkey.keys == expectedFieldsToValue.keys)
    parsedRowkey
      .map(p => (p._2.asInstanceOf[Array[Byte]], expectedFieldsToValue(p._1)))
      .foreach{
        case (actual, expected) => assert(actual.sameElements(expected))
      }
  }

  test("Compound row keys with byte fields NOT in the last field throw error on setup") {
    var rowKeyDef = "var_length:fixed_length"

    val keyFields = Seq(
      Field(
        sparkColName = "s1",
        btColFamily = "rowkey",
        btColName = "var_length",
        simpleType = Option("binary"),
        len = -1
      ),
      Field(
        sparkColName = "s2",
        btColFamily = "rowkey",
        btColName = "fixed_length",
        simpleType = Option("binary"),
        len = 8
      )
    )

    intercept[IllegalArgumentException] {
      RowKey(rowKeyDef, keyFields)
    }
  }

  test("A field without a corresponding segment throws an error") {
    val rowKeyDef = "segment1:segment2"
    val keyFields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "segment1",
        Some("string")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "segment2",
        Some("string")
      ),
      Field(
        "spark_col3",
        "rowkey",
        "segment3",
        Some("string")
      )
    )

    intercept[IllegalArgumentException] {
      RowKey(rowKeyDef, keyFields)
    }
  }

  test("A segment without a corresponding field throws an error") {
    val rowKeyDef = "segment1:segment2:segment3"
    val keyFields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "segment1",
        Some("string")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "segment2",
        Some("string")
      )
    )

    intercept[IllegalArgumentException] {
      RowKey(rowKeyDef, keyFields)
    }
  }

  test("Fields are properly sorted") {
    val rowKeyDef = "first_segment:second_segment:third_segment"
    val keyFields = Seq(
      Field(
        "spark_col2",
        "rowkey",
        "second_segment",
        Some("string")
      ),
      Field(
        "spark_col3",
        "rowkey",
        "third_segment",
        Some("string")
      ),
      Field(
        "spark_col1",
        "rowkey",
        "first_segment",
        Some("string")
      )
    )

    val expectedSortedKeyFields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "first_segment",
        Some("string")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "second_segment",
        Some("string")
      ),
      Field(
        "spark_col3",
        "rowkey",
        "third_segment",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, keyFields)

    assert(rowkey.sortedFields == expectedSortedKeyFields)
  }

  test("Compound row key converts any filters to Range.all") {
    val rowKeyDef = "seg1:seg2"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "seg2",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = EqualTo("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    assert(actual.encloses(Range.all()))
  }

  test("Non compound row key converts EqualTo filter to a single row") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = EqualTo("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expected = TreeRangeSet.create[RowKeyWrapper]()
    expected.add(Range.singleton(new RowKeyWrapper(BytesConverter.toBytes("foo"))))

    // Contains the key
    assert(actual.enclosesAll(expected))

    // Does not contain everything else
    assert(actual.complement().enclosesAll(expected.complement()))
  }

  test("Non compound row key converts LessThan filter") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = LessThan("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedNonInclusiveUpperBound = new RowKeyWrapper(BytesConverter.toBytes("foo"))

    assert(actual.encloses(Range.lessThan(expectedNonInclusiveUpperBound)))
    assert(!actual.intersects(Range.atLeast(expectedNonInclusiveUpperBound)))
  }

  test("Non compound row key converts LessThanOrEqual filter") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = LessThanOrEqual("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedInclusiveUpperBound = new RowKeyWrapper(BytesConverter.toBytes("foo"))

    assert(actual.encloses(Range.atMost(expectedInclusiveUpperBound)))
    assert(!actual.intersects(Range.greaterThan(expectedInclusiveUpperBound)))
  }

  test("Non compound row key converts GreaterThan filter") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = GreaterThan("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedNonInclusiveLowerBound = new RowKeyWrapper(BytesConverter.toBytes("foo"))

    assert(actual.encloses(Range.greaterThan(expectedNonInclusiveLowerBound)))
    assert(!actual.intersects(Range.atMost(expectedNonInclusiveLowerBound)))
  }

  test("Non compound row key converts GreaterThanOrEqual filter") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = GreaterThanOrEqual("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedInclusiveLowerBound = new RowKeyWrapper(BytesConverter.toBytes("foo"))

    assert(actual.encloses(Range.atLeast(expectedInclusiveLowerBound)))
    assert(!actual.intersects(Range.lessThan(expectedInclusiveLowerBound)))
  }

  test("Non compound row key converts StartsWith filter") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = StringStartsWith("spark_col1", "foo")

    val actual = rowkey.convertFilterToRangeSet(filter)

    val fooBytes = BytesConverter.toBytes("foo")
    val incrementedFooBytes = Utils.incrementByteArray(fooBytes)

    val inclusiveLowerBound = new RowKeyWrapper(fooBytes)
    val exclusiveUpperBound = new RowKeyWrapper(incrementedFooBytes)
    val expectedRange = Range.closedOpen(
      inclusiveLowerBound,
      exclusiveUpperBound
    )

    assert(actual.encloses(expectedRange))
    assert(!actual.intersects(Range.lessThan(inclusiveLowerBound)))
    assert(!actual.intersects(Range.atLeast(exclusiveUpperBound)))
  }

  test("Non compound row key converts In filter") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = In("spark_col1", Array("foo", "bar"))

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedRange = TreeRangeSet.create[RowKeyWrapper]()
    expectedRange.add(Range.singleton(new RowKeyWrapper(BytesConverter.toBytes("foo"))))
    expectedRange.add(Range.singleton(new RowKeyWrapper(BytesConverter.toBytes("bar"))))

    assert(actual.enclosesAll(expectedRange))
    assert(actual.complement().enclosesAll(expectedRange.complement()))
  }

  test("Non compound row key converts And filter with both filters being for the row key field includes both field in the rangeset") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = And(
      GreaterThan("spark_col1", "bar"),
      LessThan("spark_col1", "foo")
    )

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedRange = TreeRangeSet.create[RowKeyWrapper]()
    expectedRange.add(Range.all())
    expectedRange.remove(Range.atMost(new RowKeyWrapper(BytesConverter.toBytes("bar"))))
    expectedRange.remove(Range.atLeast(new RowKeyWrapper(BytesConverter.toBytes("foo"))))

    assert(actual.enclosesAll(expectedRange))
    assert(actual.complement().enclosesAll(expectedRange.complement()))
  }

  test("Non compound row key converts And filter with neither filter being for the row key field doesn't include any filters on the row set") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = And(
      GreaterThan("non_key_field", "bar"),
      LessThan("non_key_field", "foo")
    )

    val actual = rowkey.convertFilterToRangeSet(filter)

    assert(actual.encloses(Range.all()))
  }

  test("Non compound row key converts And filter with only one filter for the row key as if the non" +
  "row key field filter equated to Range.all()") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = And(
      GreaterThan("spark_col1", "bar"),
      LessThan("non-row-key-field", "foo")
    )

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedRange = TreeRangeSet.create[RowKeyWrapper]()
    expectedRange.add(Range.greaterThan(new RowKeyWrapper(BytesConverter.toBytes("bar"))))

    assert(actual.enclosesAll(expectedRange))
    assert(actual.complement().enclosesAll(expectedRange.complement()))
  }

  test("Non compound row key converts Or filter with both filters being for the row key field includes both field in the rangeset") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = Or(
      LessThan("spark_col1", "bar"),
      GreaterThan("spark_col1", "foo")
    )

    val actual = rowkey.convertFilterToRangeSet(filter)

    val expectedRange = TreeRangeSet.create[RowKeyWrapper]()
    expectedRange.add(Range.lessThan(new RowKeyWrapper(BytesConverter.toBytes("bar"))))
    expectedRange.add(Range.greaterThan(new RowKeyWrapper(BytesConverter.toBytes("foo"))))

    assert(actual.enclosesAll(expectedRange))
    assert(actual.complement().enclosesAll(expectedRange.complement()))
  }

  test("Non compound row key converts Or filter with neither filter being for the row key field doesn't include any filters on the row set") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = Or(
      GreaterThan("non_key_field", "bar"),
      LessThan("non_key_field", "foo")
    )

    val actual = rowkey.convertFilterToRangeSet(filter)

    assert(actual.encloses(Range.all()))
  }

  test("Non compound row key converts Or filter with only one filter for the row key as if the non" +
    "row key field filter equated to Range.all() (which means the result is Range.all())") {
    val rowKeyDef = "seg1"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      )
    )

    val rowkey = RowKey(rowKeyDef, fields)

    val filter: Filter = Or(
      GreaterThan("spark_col1", "bar"),
      LessThan("non-row-key-field", "foo")
    )

    val actual = rowkey.convertFilterToRangeSet(filter)

    assert(actual.encloses(Range.all()))
  }

  test("Serialize non compound row keys") {
    val testData = Table(
      ("fieldType", "fieldValue", "expectedBytes"),
      (
        "binary",
        Array[Byte](10.toByte, 20.toByte, -30.toByte),
        Array[Byte](10.toByte, 20.toByte, -30.toByte)
      ),
      (
        "boolean",
        false,
        BytesConverter.toBytes(false)
      ),
      (
        "byte",
        123.toByte,
        Array[Byte](123.toByte)
      ),
      (
        "short",
        4321.toShort,
        BytesConverter.toBytes(4321.toShort)
      ),
      (
        "int",
        -123523,
        BytesConverter.toBytes(-123523)
      ),
      (
        "long",
        9885673210123L,
        BytesConverter.toBytes(9885673210123L)
      ),
      (
        "float",
        23.678012f,
        BytesConverter.toBytes(23.678012f)
      ),
      (
        "double",
        -123876.987123000123,
        BytesConverter.toBytes(-123876.987123000123)
      ),
      // Using byte '0' in string columns is allowed in non-compound row keys.
      (
        "string",
        "foo\u0000bar\u0000",
        BytesConverter.toBytes("foo\u0000bar\u0000")
      )
    )
    forAll(testData) {
      (fieldType: String, fieldValue: Any, expectedBytes: Array[Byte]) =>

        val rowKeyDef = "seg1"
        val fields = Seq(
          Field(
            "spark_col1",
            "rowkey",
            "seg1",
            Some(fieldType)
          )
        )

        val rowKey = RowKey(rowKeyDef, fields)
        val sparkRowType = StructType(
          Seq(
            StructField("spark_col1", StringType)
          )
        )

        val sparkRow = SparkRow(fieldValue)

        val actual = rowKey.getBtRowKeyBytes(sparkRow, sparkRowType)

        assert(actual.sameElements(expectedBytes))
    }
  }

  test("Gets the proper field") {
    val testData = Table(
      ("rowType", "rowKeyDef"),
      (
        StructType(
          Seq(
            StructField("spark_col1", IntegerType)
          )
        ),
        "spark_col1"
      ),
      (
        StructType(
          Seq(
            StructField("another_col", IntegerType),
            StructField("spark_col1", IntegerType)
          )
        ),
        "spark_col1"
      ),
      (
        StructType(
          Seq(
            StructField("spark_col1", IntegerType),
            StructField("another_col", IntegerType)
          )
        ),
        "spark_col1"
      ),
      (
        StructType(
          Seq(
            StructField("another_col", IntegerType),
            StructField("yet_another_col", IntegerType),
            StructField("spark_col1", IntegerType),
            StructField("and_one_more", IntegerType),
            StructField("last_col", IntegerType)
          )
        ),
        "spark_col1"
      )
    )

    forAll(testData) {
      (rowType: StructType, rowKeyDef: String) =>

        val fields = Seq(
          Field(
            rowKeyDef,
            "rowkey",
            "seg1",
            Some("int")
          )
        )

        val rowKey = RowKey("seg1", fields)

        var count = 0
        val sparkRow = SparkRow(
          rowType.fields
            .flatMap {
              case f@_ if f.name == rowKeyDef => Some(9999)
              case _ =>
                count = count + 1
                Some(count)
            }:_*)

        val actual = rowKey.getBtRowKeyBytes(sparkRow, rowType)

        assert(actual.sameElements(BytesConverter.toBytes(9999)))
    }
  }

  test("Serialize compound row keys") {
    val rowKeyDef = "seg1:seg2"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("int")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "seg2",
        Some("binary")
      )
    )

    val rowKey = RowKey(rowKeyDef, fields)
    val sparkRowType = StructType(
      Seq(
        StructField("spark_col1", IntegerType),
        StructField("spark_col2", BinaryType)
      )
    )

    val sparkRow = SparkRow(
      sparkRowType.fields
        .flatMap {
          case f@_ if f.name == "spark_col1" => Some(9999)
          case f@_ if f.name == "spark_col2" => Some(Array[Byte](10.toByte, 20.toByte, -30.toByte))
          case _ => None
        }:_*)

    val expectedBytes = BytesConverter.toBytes(9999)++Array[Byte](10.toByte, 20.toByte, -30.toByte)

    val actual = rowKey.getBtRowKeyBytes(sparkRow, sparkRowType)

    assert(actual.sameElements(expectedBytes))
  }

  test("Serialize compound row keys with variable length string") {
    val rowKeyDef = "seg1:seg2"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "seg2",
        Some("binary")
      )
    )

    val rowKey = RowKey(rowKeyDef, fields)
    val sparkRowType = StructType(
      Seq(
        StructField("spark_col1", StringType),
        StructField("spark_col2", BinaryType)
      )
    )

    val sparkRow = SparkRow(
      sparkRowType.fields
        .flatMap {
          case f@_ if f.name == "spark_col1" => Some("foo\u0000")
          case f@_ if f.name == "spark_col2" => Some(Array[Byte](10.toByte, 20.toByte, -30.toByte))
          case _ => None
        }:_*)

    val expectedBytes = BytesConverter.toBytes("foo\u0000")++Array[Byte](10.toByte, 20.toByte, -30.toByte)

    val actual = rowKey.getBtRowKeyBytes(sparkRow, sparkRowType)

    assert(actual.sameElements(expectedBytes))
  }

  test("Serialize compound row keys with variable length string throws error if no delimiter is found") {
    val rowKeyDef = "seg1:seg2"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("string")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "seg2",
        Some("binary")
      )
    )

    val rowKey = RowKey(rowKeyDef, fields)
    val sparkRowType = StructType(
      Seq(
        StructField("spark_col1", StringType),
        StructField("spark_col2", BinaryType)
      )
    )

    val sparkRow = SparkRow(
      sparkRowType.fields
        .flatMap {
          case f@_ if f.name == "spark_col1" => Some("foo")
          case f@_ if f.name == "spark_col2" => Some(Array[Byte](10.toByte, 20.toByte, -30.toByte))
          case _ => None
        }:_*)

    intercept[IllegalArgumentException](
      rowKey.getBtRowKeyBytes(sparkRow, sparkRowType)
    )
  }

  test("Serialize compound row keys with variable length string throws error " +
  "if delimiter is at the middle of the string and there are no more fields to be parsed") {
    val rowKeyDef = "seg1:seg2"
    val fields = Seq(
      Field(
        "spark_col1",
        "rowkey",
        "seg1",
        Some("int")
      ),
      Field(
        "spark_col2",
        "rowkey",
        "seg2",
        Some("string")
      )
    )

    val rowKey = RowKey(rowKeyDef, fields)
    val sparkRowType = StructType(
      Seq(
        StructField("spark_col1", IntegerType),
        StructField("spark_col2", StringType)
      )
    )

    val sparkRow = SparkRow(
      sparkRowType.fields
        .flatMap {
          case f@_ if f.name == "spark_col1" => Some(123)
          case f@_ if f.name == "spark_col2" => Some("foo\u0000bar\u0000")
          case _ => None
        }:_*)

    intercept[IllegalArgumentException](
      rowKey.getBtRowKeyBytes(sparkRow, sparkRowType)
    )
  }
}
