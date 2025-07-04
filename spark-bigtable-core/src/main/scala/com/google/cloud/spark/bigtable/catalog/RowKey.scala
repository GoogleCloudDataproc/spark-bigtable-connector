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

import com.google.cloud.spark.bigtable.datasources.Field
import com.google.cloud.spark.bigtable.filters.{EqualToFilterAdapter, GreaterThanFilterAdapter, GreaterThanOrEqualFilterAdapter, LessThanFilterAdapter, LessThanOrEqualFilterAdapter, RowKeyWrapper, StringStartsWithFilterAdapter}
import com.google.common.collect.{ImmutableRangeSet, Range, RangeSet, TreeRangeSet}
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Or, StringStartsWith}
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import org.apache.yetus.audience.InterfaceAudience
import com.google.cloud.bigtable.data.v2.models.{Row => BigtableRow}
import com.google.cloud.spark.bigtable.catalog.RowKey.COMPOUND_ROWKEY_STRING_DELIMITER
import org.apache.spark.sql.{Row => SparkRow}

object RowKey {
  // Row keys composed of multiple spark fields are allowed by separating them
  // with `:`
  private val ROW_KEY_SEGMENT_DELIMITER = ":"

  // On compound row keys, string values should always be terminated by a byte '0'
  private val COMPOUND_ROWKEY_STRING_DELIMITER: Byte = 0

  def apply(rowKeyStr: String, rowKeyfields: Seq[Field]): RowKey = {
    val sortedKeys = rowKeyStr.split(ROW_KEY_SEGMENT_DELIMITER)

    checkEveryFieldHasSegment(rowKeyfields, sortedKeys)
    checkEverySegmentHasField(sortedKeys, rowKeyfields)

    val sortedFields = sortedKeys
      .map(key => rowKeyfields.find(_.btColName == key).get)

    if (sortedFields.length > 1) {
      checkCompoundRowKeyHasNoUnsupportedVarLengthTypes(sortedFields)
      checkNoVariableLengthBinaryBeforeLastField(sortedFields)
    }

    new RowKey(sortedFields)
  }

  private def checkNoVariableLengthBinaryBeforeLastField(sortedFields: Array[Field]): Unit = {
    sortedFields
      .dropRight(1)
      .filter(_.length == -1)
      .filter(_.dt == BinaryType)
      .foreach(f => throw new IllegalArgumentException(
        "Variable length binary fields may only be used as the last field "
          + f"for compound row keys. Field: ${f.sparkColName}"
      ))
  }

  private def checkCompoundRowKeyHasNoUnsupportedVarLengthTypes(sortedFields: Array[Field]): Unit = {
    sortedFields
      .filter(_.length == -1)
      .filter(field => field.dt != BinaryType && field.dt != StringType)
      .foreach(f => throw new IllegalArgumentException(
        f"Variable length field of type ${f.dt} in field ${f.sparkColName} is "
          + "not supported for compound row keys"
      ))
  }

  private def checkEverySegmentHasField(sortedKeys: Array[String], rowKeyfields: Seq[Field]): Unit = {
    sortedKeys
      .filter(key => !rowKeyfields.exists(_.btColName == key))
      .foreach(key =>
        throw new IllegalArgumentException(
          f"No column definition for row key segment $key"
        ))
  }

  private def checkEveryFieldHasSegment(rowKeyfields: Seq[Field], sortedKeys: Array[String]): Unit = {
    rowKeyfields
      .filter(field => !sortedKeys.contains(field.btColName))
      .foreach(field => throw new IllegalArgumentException(
        f"Column definition for row key without a segment: $field"
      ))
  }
}

@InterfaceAudience.Private
case class RowKey(sortedFields: Seq[Field]) {
  private val fieldsMapOnSparkCol = sortedFields.map(f =>
    (f.sparkColName, f)).toMap

  private val isCompound = sortedFields.length > 1

  def convertFilterToRangeSet(filter: Filter): RangeSet[RowKeyWrapper] = {
    if (isCompound) {
      return ImmutableRangeSet.of(Range.all[RowKeyWrapper]())
    }
    filter match {
      case EqualTo(attribute, value) if fieldsMapOnSparkCol.contains(attribute) =>
        EqualToFilterAdapter.convertValueToRangeSet(value)
      case LessThan(attribute, value) if fieldsMapOnSparkCol.contains(attribute) =>
          LessThanFilterAdapter.convertValueToRangeSet(value)
      case LessThanOrEqual(attribute, value) if fieldsMapOnSparkCol.contains(attribute) =>
        LessThanOrEqualFilterAdapter.convertValueToRangeSet(value)
      case GreaterThan(attribute, value) if fieldsMapOnSparkCol.contains(attribute) =>
        GreaterThanFilterAdapter.convertValueToRangeSet(value)
      case GreaterThanOrEqual(attribute, value) if fieldsMapOnSparkCol.contains(attribute) =>
        GreaterThanOrEqualFilterAdapter.convertValueToRangeSet(value)
      case StringStartsWith(attribute, value) if fieldsMapOnSparkCol.contains(attribute) =>
        StringStartsWithFilterAdapter.convertValueToRangeSet(value)
      case In(attribute, values) if fieldsMapOnSparkCol.contains(attribute) =>
        val union = TreeRangeSet.create[RowKeyWrapper]()
        values.foreach(v => union.addAll(EqualToFilterAdapter.convertValueToRangeSet(v)))
        ImmutableRangeSet.copyOf(union)
      case And(left, right) =>
        val intersectionRange: RangeSet[RowKeyWrapper] =
          TreeRangeSet.create[RowKeyWrapper](convertFilterToRangeSet(left))
        intersectionRange.removeAll(convertFilterToRangeSet(right).complement())
        ImmutableRangeSet.copyOf(intersectionRange)
      case Or(left, right) =>
        val unionRange: RangeSet[RowKeyWrapper] =
          TreeRangeSet.create[RowKeyWrapper](convertFilterToRangeSet(left))
        unionRange.addAll(convertFilterToRangeSet(right))
        ImmutableRangeSet.copyOf(unionRange)
      case _ =>
        ImmutableRangeSet.of(Range.all[RowKeyWrapper]())
    }
  }

  // The int on the return type represents how many bytes were consumed
  private def parseFieldFromBytes(field: Field, row: Array[Byte], offset: Int): (Any, Int) = {
    if (field.length != -1) {
      (field.bigtableToScalaValue(row, offset, field.length),
        field.length)
    } else {
      // String types with variable length need to be validated
      field.dt match {
        case StringType if isCompound =>
          val delimiterPos = row.indexOf(COMPOUND_ROWKEY_STRING_DELIMITER, offset)
          if (delimiterPos == -1) {
            throw new IllegalArgumentException(
              "Error when parsing row key [" + row.mkString(
                ", "
              ) + "] to DataFrame column "
                + field + ". " + "When using compound row keys, a String type column "
                + "should have a fixed length or have *exactly one* delimiter character "
                + "(byte '0') at the end."
            )
          }
          val valueLength = delimiterPos - offset + 1
          (field.bigtableToScalaValue(row, offset, valueLength),
            valueLength)
        case _ =>
          // Consume until the end
          val valueLength = row.length - offset
          (field.bigtableToScalaValue(row, offset, valueLength),
            valueLength)
      }
    }
  }

  def parseFieldsFromBtRow(bigtableRow: BigtableRow): Map[Field, Any] = {
    val row: Array[Byte] = bigtableRow.getKey.toByteArray
    val (totalConsumedBytes, result) = sortedFields
      .foldLeft((0, Seq.empty[(Field, Any)])) {
        case ((offset, parsedFields), currentField) =>
          val (value, parsedBytes) = parseFieldFromBytes(currentField, row, offset)
          (offset + parsedBytes, parsedFields :+ (currentField -> value))
      }

    if (totalConsumedBytes < row.length) {
      throw new IllegalArgumentException(
        f"Error when parsing row key [${row.mkString(", ")}] to DataFrame "
          + f"column. Only $totalConsumedBytes were consumed out of "
          + f"${row.length}.")
    }

    result.toMap
  }

  def getBtRowKeyBytes(sparkRow: SparkRow, sparkRowType: StructType): Array[Byte] = {
    sortedFields.flatMap { field =>
      val value = sparkRow.get(sparkRowType.fieldIndex(field.sparkColName))
      val bytes = field.scalaValueToBigtable(value)

      if (isCompound && field.dt == StringType && field.length == -1) {
        val delimiterIndex = bytes.indexOf(COMPOUND_ROWKEY_STRING_DELIMITER)
        if (delimiterIndex != bytes.length - 1) {
          throw new IllegalArgumentException(
            s"Error writing row $sparkRow. In a compound row key, variable-length string column " +
              s"'${field.sparkColName}' must be terminated by a single null byte at the end."
          )
        }
      }
      bytes
    }.toArray
  }
}
