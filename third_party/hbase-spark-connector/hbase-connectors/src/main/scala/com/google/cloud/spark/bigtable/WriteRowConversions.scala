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
import com.google.cloud.spark.bigtable.datasources.{BigtableTableCatalog, BytesConverter, Field}
import com.google.protobuf.ByteString
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row => SparkRow}

class WriteRowConversions(
    catalog: BigtableTableCatalog,
    schema: StructType,
    writeTimestampMicros: Long
) extends Serializable {

  private val columnIndexAndField: Array[(Int, Field)] = catalog
    .sMap
    .fields
    .filter(!_.isRowKey)
    .map(field => (schema.fieldIndex(field.sparkColName), field))
    .toArray

  /** Converts a Spark SQL Row to a Bigtable RowMutationEntry.
    *
    * @param row       The Spark SQL row
    * @return          A RowMutationEntry corresponding to the Spark SQL Row
    */
  def convertToBigtableRowMutation(row: SparkRow): RowMutationEntry = {
    val rowKeyBytes: Seq[Byte] = catalog.row.getBtRowKeyBytes(row, catalog.toDataType)

    val mutation = RowMutationEntry.create(ByteString.copyFrom(rowKeyBytes.toArray))
    columnIndexAndField.map { case (index, field) =>
      val columnValue = row(index)
      if (columnValue != null) {
        val columnValueBytes = field.scalaValueToBigtable(columnValue)

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
