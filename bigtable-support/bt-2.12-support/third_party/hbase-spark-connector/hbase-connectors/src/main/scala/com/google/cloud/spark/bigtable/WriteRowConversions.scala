/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This class's logic is inspired by the hbase-spark connector, but modified to add
// new functionality and fix bugs. The entire logic is still kept inside the third_party
// directory for licensing compliance.

package com.google.cloud.spark.bigtable

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry
import com.google.cloud.spark.bigtable.datasources.{BigtableTableCatalog, BytesConverter, Field, Utils}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row => SparkRow}

class WriteRowConversions(
    catalog: BigtableTableCatalog,
    schema: StructType,
    writeTimestampMicros: Long
) extends Serializable {
  private val rowKeyFields: Seq[Field] = catalog.getRowKeyColumns
  // Defined as non-private and var (instead of val) for unit testing.
  @VisibleForTesting
  var rowKeyIndexAndFields: Seq[(Int, Field)] = rowKeyFields.map {
    case rowKeyField =>
      (schema.fieldIndex(rowKeyField.sparkColName), rowKeyField)
  }
  @VisibleForTesting
  var columnIndexAndField: Array[(Int, Field)] = schema.fieldNames
    .partition(fieldName =>
      rowKeyFields.map(_.sparkColName).contains(fieldName)
    )
    ._2
    .map(fieldName =>
      (schema.fieldIndex(fieldName), catalog.getField(fieldName))
    )

  /** Validate the row key values and converts them to a byte array.
    *
    * @param rowKeyColumnsBytes    A sequence of byte arrays corresponding to each
    *                                column that's part of the (compound) row key
    * @param sparkRow              The Spark SQL row
    * @param catalog               The catalog for converting from Spark SQL
    *                                DataFrame to a Bigtable table
    * @return                      A byte array corresponding to the Bigtable row key
    */
  private def validateAndConvertRowKeyForWrite(
      rowKeyColumnsBytes: Seq[Array[Byte]],
      sparkRow: SparkRow,
      catalog: BigtableTableCatalog
  ): Array[Byte] = {
    // Construct the row key byte array.
    val rowKeyLen = rowKeyColumnsBytes.foldLeft(0) {
      case (currentLen, rowKeyColumn) =>
        currentLen + rowKeyColumn.length
    }
    val rowKeyBytes = new Array[Byte](rowKeyLen)
    var offset = 0

    (rowKeyColumnsBytes zip rowKeyIndexAndFields).foreach {
      case (columnValueBytes, idxAndField) =>
        // For compound row keys, we first need to validate column values.
        if (catalog.hasCompoundRowKey) {
          val field = idxAndField._2
          field.dt match {
            // Currently, only string columns with a dynamic length have to be validated.
            case StringType =>
              if (field.length == -1) {
                val delimPos =
                  columnValueBytes.indexOf(BigtableTableCatalog.delimiter)
                if (delimPos == -1 || delimPos < columnValueBytes.length - 1) {
                  throw new IllegalArgumentException(
                    "Error when writing DataFrame column " + field + " in row "
                      + sparkRow + ". " + "When using compound row keys, a String type column "
                      + "should have a fixed length or have *exactly one* delimiter character (byte '0') at the end."
                  )
                }
              }
            // All other types either have a fixed length (e.g., double), or can only be at the
            // end of the row key (i.e., byte array) and are validated when creating the catalog).
            case _ =>
          }
        }
        System.arraycopy(
          columnValueBytes,
          0,
          rowKeyBytes,
          offset,
          columnValueBytes.length
        )
        offset += columnValueBytes.length
    }
    rowKeyBytes
  }

  /** Converts a Spark SQL Row to a Bigtable RowMutationEntry.
    *
    * @param row       The Spark SQL row
    * @return          A RowMutationEntry corresponding to the Spark SQL Row
    */
  def convertToBigtableRowMutation(row: SparkRow): RowMutationEntry = {
    val rowKeyColumnsBytes: Seq[Array[Byte]] = rowKeyIndexAndFields.map {
      case (index, field) =>
        Utils.toBytes(row(index), field)
    }
    val rowKeyBytes: Array[Byte] =
      validateAndConvertRowKeyForWrite(rowKeyColumnsBytes, row, catalog)

    val mutation = RowMutationEntry.create(ByteString.copyFrom(rowKeyBytes))
    columnIndexAndField.map { case (index, field) =>
      val columnValue = row(index)
      if (columnValue != null) {
        val columnValueBytes = Utils.toBytes(columnValue, field)
        val columnNameBytes = BytesConverter.toBytes(field.btColName)
        mutation.setCell(
          field.btColFamily,
          ByteString.copyFrom(columnNameBytes),
          writeTimestampMicros,
          ByteString.copyFrom(columnValueBytes)
        )
      }
    }
    mutation
  }
}
