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

import com.google.cloud.spark.bigtable.datasources.{BigtableTableCatalog, DataTypeBytes, DataTypeParserWrapper}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class BigtableCatalogSuite extends AnyFunSuite with Logging {
  val catalog = s"""{
                   |"table":{"name":"htable"},
                   |"rowkey":"key1:key2",
                   |"columns":{
                   |"col1":{"cf":"rowkey", "col":"key1", "type":"string"},
                   |"col2":{"cf":"rowkey", "col":"key2", "type":"long"},
                   |"col3":{"cf":"cf1", "col":"col3", "type":"binary"},
                   |"col4":{"cf":"cf1", "col":"col4", "type":"double"}
                   |}
                   |}""".stripMargin
  val parameters = Map("catalog" -> catalog)
  val t = BigtableTableCatalog(parameters)
  def checkDataType(
      dataTypeString: String,
      expectedDataType: DataType
  ): Unit = {
    test(s"parse ${dataTypeString.replace("\n", "")}") {
      assert(DataTypeParserWrapper.parse(dataTypeString) === expectedDataType)
    }
  }

  test("basic") {
    assert(t.getField("col1").isRowKey)
    assert(t.getField("col2").dt == LongType)
    assert(t.getField("col3").dt == BinaryType)
    assert(t.getField("col4").dt == DoubleType)
    assert(t.getField("col1").isRowKey)
    assert(t.getField("col2").isRowKey)
    assert(!t.getField("col3").isRowKey)
    assert(t.getField("col2").length == DataTypeBytes.LONG_BYTES)
    assert(t.getField("col1").length == -1)
  }

  test("compound row key with binary types") {
    // When using compound row keys, a binary-type column should have a fixed
    // length or be the last column in the row key.
    val catalog = s"""{
                     |"table":{"name":"htable2"},
                     |"rowkey":"binary_key1:long_key:binary_key2",
                     |"columns":{
                     |"binary_col1":{"cf":"rowkey", "col":"binary_key1", "type":"binary", "length":"10"},
                     |"long_col":{"cf":"rowkey", "col":"long_key", "type":"long"},
                     |"binary_col2":{"cf":"rowkey", "col":"binary_key2", "type":"binary"},
                     |"col4":{"cf":"cf1", "col":"col4", "type":"long"}
                     |}
                     |}""".stripMargin
    val parameters = Map("catalog" -> catalog)
    BigtableTableCatalog(parameters)
  }

  test("unsupported type") {
    val catalog = s"""{
                     |"table":{"name":"htable2"},
                     |"rowkey":"timestamp_key",
                     |"columns":{
                     |"timestamp_col":{"cf":"rowkey", "col":"timestamp_key", "type":"timestamp"},
                     |"col3":{"cf":"cf1", "col":"col3", "type":"long"}
                     |}
                     |}""".stripMargin
    val parameters = Map("catalog" -> catalog)
    intercept[IllegalArgumentException] {
      BigtableTableCatalog(parameters)
    }
  }

  test("invalid compound row key with binary type") {
    // When using compound row keys, a binary-type column should have a fixed
    // length or be the last column in the row key.
    val catalog = s"""{
                     |"table":{"name":"htable2"},
                     |"rowkey":"binary_key:long_key",
                     |"columns":{
                     |"binary_col":{"cf":"rowkey", "col":"binary_key", "type":"binary"},
                     |"long_col":{"cf":"rowkey", "col":"long_key", "type":"long"},
                     |"col3":{"cf":"cf1", "col":"col3", "type":"long"}
                     |}
                     |}""".stripMargin
    val parameters = Map("catalog" -> catalog)
    intercept[IllegalArgumentException] {
      BigtableTableCatalog(parameters)
    }
  }

  test("no columns specified as row key") {
    // When using compound row keys, a binary-type column should have a fixed
    // length or be the last column in the row key.
    val catalog = s"""{
                     |"table":{"name":"htable2"},
                     |"rowkey":"col1",
                     |"columns":{
                     |"col1":{"cf":"not_rowkey", "col":"col1", "type":"long"},
                     |"col2":{"cf":"not_rowkey", "col":"col2", "type":"long"}
                     |}
                     |}""".stripMargin
    val parameters = Map("catalog" -> catalog)
    intercept[IllegalArgumentException] {
      BigtableTableCatalog(parameters)
    }
  }

  test("mismatch in number of row key columns") {
    // When using compound row keys, a binary-type column should have a fixed
    // length or be the last column in the row key.
    val catalog = s"""{
                     |"table":{"name":"htable2"},
                     |"rowkey":"col1",
                     |"columns":{
                     |"col1":{"cf":"rowkey", "col":"col1", "type":"long"},
                     |"col2":{"cf":"rowkey", "col":"col2", "type":"long"}
                     |}
                     |}""".stripMargin
    val parameters = Map("catalog" -> catalog)
    intercept[IllegalArgumentException] {
      BigtableTableCatalog(parameters)
    }
  }
}
