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

import com.google.cloud.spark.bigtable.catalog.CatalogDefinition.{ColumnsDefinition, RegexColumnsDefinition, RowKeyDefinition}
import upickle.default.Reader

import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON

object CatalogDefinition {
  // This is the key for defining a bigtable catalog on Spark's configuration
  val CATALOG_KEY = "catalog"

  object TABLE {
    val KEY = "table"
    val TABLE_NAME_KEY = "name"
  }

  val ROW_KEY = "rowkey"

  object COLUMNS {
    val KEY = "columns"
    val COLUMN_FAMILY_KEY = "cf"
    val COLUMN_QUALIFIER_KEY = "col"
    val TYPE_KEY = "type"
    val AVRO_TYPE_NAME_KEY = "avro"
    val LENGTH_KEY = "length"
  }

  object REGEX_COLUMNS {
    val KEY = "regexColumns"
    val COLUMN_FAMILY_KEY = "cf"
    val REGEX_PATTERN_KEY = "pattern"
    val TYPE_KEY = "type"
    val AVRO_TYPE_NAME_KEY = "avro"
    val LENGTH_KEY = "length"
  }

  // params is the full configuration map from Spark
  def apply(params: Map[String, String]): CatalogDefinition = {
    val catalogDefinitionJsonString =
      params.getOrElse(CATALOG_KEY, throw new IllegalArgumentException(
        "Bigtable catalog definition not found"))


    implicit val tableReader: Reader[TableDefinition] = upickle.default.macroR[TableDefinition]
    implicit val columnDefinitionReader: Reader[ColumnDefinition] = upickle.default.macroR[ColumnDefinition]
    implicit val regexColumnDefinitionReader: Reader[RegexColumnDefinition] = upickle.default.macroR[RegexColumnDefinition]
    implicit val catalogReader: Reader[CatalogDefinition] = upickle.default.macroR[CatalogDefinition]

    Try(upickle.default.read[CatalogDefinition](catalogDefinitionJsonString)) match {
      case Success(result) => result
      case Failure(exception) => throw new IllegalArgumentException(
        "Error when parsing the bigtable catalog configuration", exception
      )
    }
  }

  type RowKeyDefinition = String
  type ColumnsDefinition = Map[String, ColumnDefinition]
  type RegexColumnsDefinition = Map[String, RegexColumnDefinition]
}

case class CatalogDefinition(table: TableDefinition,
                             rowkey: RowKeyDefinition,
                             columns: ColumnsDefinition,
                             regexColumns: Option[RegexColumnsDefinition] = None)

case class TableDefinition(name: String)

case class ColumnDefinition(cf: Option[String] = None,
                            col: String,
                            `type`: Option[String] = None,
                            avro: Option[String] = None,
                            length: Option[String] = None)

case class RegexColumnDefinition(cf: String,
                       pattern: String,
                       `type`: Option[String] = None,
                       avro: Option[String] = None,
                       length: Option[String] = None)
