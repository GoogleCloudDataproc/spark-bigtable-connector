package com.google.cloud.spark.bigtable.catalog

import com.google.cloud.spark.bigtable.Logging
import org.scalatest.funsuite.AnyFunSuite

class CatalogDefinitionTest extends AnyFunSuite with Logging {
  test("All properties are parsed") {
    val catalog =
      s"""{
         |"table": {
         |  "name": "expected-table-name"
         |},
         |"rowkey": "expected-row-key",
         |"columns": {
         |  "expected-col-1": {
         |    "cf": "expected-cf-1",
         |    "col": "expected-col-1",
         |    "type": "expected-type-1",
         |    "avro": "expected-avro-1",
         |    "length": 1
         |  },
         |  "expected-col-2": {
         |    "cf": "expected-cf-2",
         |    "col": "expected-col-2",
         |    "type":"expected-type-2",
         |    "avro": "expected-avro-2",
         |    "length": 2
         |  }
         |},
         |"regexColumns": {
         |  "expected-regex-col-1": {
         |    "cf": "expected-regex-cf-1",
         |    "pattern": "expected-regex-col-1",
         |    "type": "expected-regex-type-1",
         |    "avro": "expected-avro-3",
         |    "length": 3
         |  },
         |  "expected-regex-col-2": {
         |    "cf": "expected-regex-cf-2",
         |    "pattern": "expected-regex-col-2",
         |    "type":"expected-regex-type-2",
         |    "avro": "expected-avro-4",
         |    "length": 4
         |  }
         |}
         |}"""

    val actualDefinition = CatalogDefinition(Map(CatalogDefinition.CATALOG_KEY -> catalog))

    assert(actualDefinition.table.name == "expected-table-name")
    assert(actualDefinition.rowkey == "expected-row-key")
    assert(actualDefinition.columns == ColumnsDefinition(
      Map(
        "expected-col-1" -> ColumnDefinition(
          Some("expected-cf-1"),
          "expected-col-1",
          Some("expected-type-1"),
          Some("expected-avro-1"),
          Some(1)
        ),
        "expected-col-2" -> ColumnDefinition(
          Some("expected-cf-2"),
          "expected-col-2",
          Some("expected-type-2"),
          Some("expected-avro-2"),
          Some(2)
        )
      )
    ))
    assert(actualDefinition.regexColumns.contains(RegexColumnsDefinition(
      Map(
        "expected-regex-col-1" -> RegexColumnDefinition(
          "expected-regex-cf-1",
          "expected-regex-col-1",
          Some("expected-regex-type-1"),
          Some("expected-avro-3"),
          Some(3)
        ),
        "expected-col-2" -> RegexColumnDefinition(
          "expected-regex-cf-2",
          "expected-regex-col-2",
          Some("expected-regex-type-2"),
          Some("expected-avro-4"),
          Some(4)
        )
      )
    )))
  }
}
