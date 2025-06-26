package com.google.cloud.spark.bigtable.catalog

import com.google.cloud.spark.bigtable.catalog.CatalogDefinition.RowKeyDefinition
import com.google.cloud.spark.bigtable.datasources.BigtableTableCatalog.tableCatalog

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
    val catalogDefinitionJson =
      params.getOrElse(CATALOG_KEY, throw new IllegalArgumentException(
        "Bigtable catalog definition not found"))

    val catalogDefinitionMap = JSON
      .parseFull(catalogDefinitionJson) match {
      case Some(map: Map[String, Any]) => map
      case _ => throw new IllegalArgumentException(
        s"Could not parse bigtable catalog definition: $catalogDefinitionJson"
      )
    }

    val tableDefinition = getTableDefinition(catalogDefinitionMap)
    val rowKey = catalogDefinitionMap
      .getOrElse(ROW_KEY, throw new IllegalArgumentException(
        f"Missing entry for $ROW_KEY in the bigtable catalog definition"
      )).asInstanceOf[RowKeyDefinition]
    val columnsDefinition = getColumnsDefinition(catalogDefinitionMap)
    val regexColumnsDefinition = getRegexColumnsDefinition(catalogDefinitionMap)

    new CatalogDefinition(tableDefinition, rowKey, columnsDefinition, regexColumnsDefinition)
  }

  private def getTableDefinition(
      catalogDefinitionMap: Map[String, Any]): TableDefinition = {
    val tableDefinition = catalogDefinitionMap
      .getOrElse(TABLE.KEY, throw new IllegalArgumentException(
        f"Missing entry for ${TABLE.KEY} in the Bigtable catalog"
      )).asInstanceOf[Map[String, _]]

    val tableName = tableDefinition
      .getOrElse(TABLE.TABLE_NAME_KEY, throw new IllegalArgumentException(
        f"Missing entry for ${TABLE.TABLE_NAME_KEY} in the Bigtable catalog"
      )).asInstanceOf[String]

    TableDefinition(tableName)
  }

  private def getColumnsDefinition(
      catalogDefinitionMap: Map[String, Any]): ColumnsDefinition = {
    val columnsDefinition = catalogDefinitionMap
      .getOrElse(COLUMNS.KEY, throw new IllegalArgumentException(
      f"Missing entry for ${COLUMNS.KEY} in the Bigtable catalog"
    )).asInstanceOf[Map[String, Map[String, String]]]

    ColumnsDefinition(columnsDefinition.map(columnDefinition =>
      (columnDefinition._1, getColumnDefinition(columnDefinition._1, columnDefinition._2)
    )))
  }

  private def getColumnDefinition(
      columnName: String,
      columnDefinition: Map[String, String]): ColumnDefinition = {
    val columnFamily = columnDefinition
      .get(COLUMNS.COLUMN_FAMILY_KEY)

    val columnQualifier = columnDefinition
      .getOrElse(COLUMNS.COLUMN_QUALIFIER_KEY,
        throw new IllegalArgumentException(f"Missing " +
          f"${COLUMNS.COLUMN_QUALIFIER_KEY} entry for column $columnName"))

    val maybeType = columnDefinition.get(COLUMNS.TYPE_KEY)
    val maybeAvro = columnDefinition.get(COLUMNS.AVRO_TYPE_NAME_KEY)

    val maybeLength = columnDefinition
      .get(COLUMNS.LENGTH_KEY)
      .map(_.toInt)

    ColumnDefinition(columnFamily, columnQualifier, maybeType, maybeAvro, maybeLength)
  }

  private def getRegexColumnsDefinition(
      catalogDefinitionMap: Map[String, Any]): Option[RegexColumnsDefinition] = {
    catalogDefinitionMap.get(REGEX_COLUMNS.KEY) match {
      case Some(regexColumnsDefinition: Map[String, Map[String, String]]) =>
        Some(
          RegexColumnsDefinition(
            regexColumnsDefinition.map(
              columnDefinition =>
                (columnDefinition._1,
                  getRegexColumnDefinition(columnDefinition._1, columnDefinition._2)
          ))))
      case _ => None
    }
  }

  private def getRegexColumnDefinition(
      columnName: String,
      columnDefinition: Map[String, String]): RegexColumnDefinition = {
    val columnFamily = columnDefinition
      .getOrElse(REGEX_COLUMNS.COLUMN_FAMILY_KEY,
        throw new IllegalArgumentException(f"Missing " +
          f"${REGEX_COLUMNS.COLUMN_FAMILY_KEY} entry for regex column $columnName"))

    val pattern = columnDefinition
      .getOrElse(REGEX_COLUMNS.REGEX_PATTERN_KEY,
        throw new IllegalArgumentException(f"Missing " +
          f"${REGEX_COLUMNS.REGEX_PATTERN_KEY} entry for regex column $columnName"))

    val maybeType = columnDefinition.get(REGEX_COLUMNS.TYPE_KEY)
    val maybeAvro = columnDefinition.get(REGEX_COLUMNS.AVRO_TYPE_NAME_KEY)

    val maybeLength = columnDefinition
      .get(REGEX_COLUMNS.LENGTH_KEY)
      .map(_.toInt)

    RegexColumnDefinition(columnFamily, pattern, maybeType, maybeAvro, maybeLength)
  }

  type RowKeyDefinition = String
}

case class CatalogDefinition(table: TableDefinition,
                             rowkey: RowKeyDefinition,
                             columns: ColumnsDefinition,
                             regexColumns: Option[RegexColumnsDefinition])

case class TableDefinition(name: String)

case class ColumnsDefinition(columns: Map[String, ColumnDefinition])

case class ColumnDefinition(cf: Option[String],
                            col: String,
                            `type`: Option[String],
                            avro: Option[String],
                            length: Option[Int])

case class RegexColumnsDefinition(columns: Map[String, RegexColumnDefinition])

case class RegexColumnDefinition(cf: String,
                       pattern: String,
                       `type`: Option[String],
                       avro: Option[String],
                       length: Option[Int])