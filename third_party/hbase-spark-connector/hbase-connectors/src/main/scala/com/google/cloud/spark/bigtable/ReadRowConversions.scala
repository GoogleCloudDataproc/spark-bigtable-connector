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

package com.google.cloud.spark.bigtable

import com.google.cloud.bigtable.data.v2.models.{Row => BigtableRow}
import com.google.cloud.spark.bigtable.datasources.{BigtableTableCatalog, Field}
import org.apache.spark.sql.{Row => SparkRow}

object ReadRowConversions extends Serializable {
  /** Converts a Bigtable Row to a Spark SQL Row.
    *
    * @param fields       The fields required by Spark
    * @param bigtableRow  The Bigtable row
    * @param catalog      The catalog for converting from Spark SQL
    *                       DataFrame to a Bigtable table
    * @return             A Spark SQL row containing all of the required columns
    */
  def buildRow(
      fields: Seq[String],
      bigtableRow: BigtableRow,
      catalog: BigtableTableCatalog
  ): SparkRow = {
    catalog.convertBtRowToSparkRow(bigtableRow, fields)
  }
}
