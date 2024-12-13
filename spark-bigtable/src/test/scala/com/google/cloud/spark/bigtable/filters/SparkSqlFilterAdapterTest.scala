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

package com.google.cloud.spark.bigtable.filters

import com.google.cloud.spark.bigtable.BigtableRelation
import com.google.cloud.spark.bigtable.datasources._
import com.google.common.collect.RangeSet
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Not, Or, StringStartsWith}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class SparkSqlFilterAdapterTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {
  val stringRowKeyCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol",
       |"columns":{
       |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"},
       |"longCol":{"cf":"col_family1", "col":"longCol", "type":"long"}
       |}
       |}""".stripMargin
  val longRowKeyCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"longCol",
       |"columns":{
       |"stringCol":{"cf":"col_family1", "col":"stringCol", "type":"string"},
       |"longCol":{"cf":"rowkey", "col":"longCol", "type":"long"}
       |}
       |}""".stripMargin
  val byteArrayRowKeyCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"binaryCol",
       |"columns":{
       |"binaryCol":{"cf":"rowkey", "col":"binaryCol", "type":"binary"},
       |"longCol":{"cf":"col_family1", "col":"longCol", "type":"long"}
       |}
       |}""".stripMargin

  @transient var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf
    sc = new SparkContext("local", "test", sparkConf)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  // Should be converted to
  // (-inf, "bbbVal") \\union ["somprefix", "someprefiy") \\union ["yyyVal", +inf).
  test("stringRowKeyOnlyWithOr") {
    val relation =
      BigtableRelation(createParametersMap(stringRowKeyCatalog), None)(
        sqlContext
      )
    val filter: Filter = Or(
      Or(
        LessThan("stringCol", "bbbVal"),
        GreaterThanOrEqual("stringCol", "yyyVal")
      ),
      StringStartsWith("stringCol", "someprefix")
    )
    val valuesInRangeSet: Array[String] =
      Array[String]("aaa", "someprefix", "someprefix_foo", "yyyVal", "zzz")
    val valuesOutsideRangeSet: Array[String] =
      Array[String]("bbbVal", "somepref_not", "someprefiy", "yyySmall")

    val resultRangeSet: RangeSet[RowKeyWrapper] =
      SparkSqlFilterAdapter.createRowKeyRangeSet(
        Array[Filter](filter),
        relation.catalog,
        pushDownRowKeyFilters = true
      )

    verifyInRangeSet(
      valuesInRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
    verifyOutsideRangeSet(
      valuesOutsideRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
  }

  // Should be converted to
  // ["aaaBar", "aaaBar"] \\union ["bbbPrefix", "bbbPrefixFoo") \\union ("wwwVal", +inf).
  test("stringRowKeyMixAndOr") {
    val relation =
      BigtableRelation(createParametersMap(stringRowKeyCatalog), None)(
        sqlContext
      )
    val filter: Filter = Or(
      Or(
        And(
          GreaterThan("stringCol", "wwwVal"),
          LessThanOrEqual("stringCol", "yyyVal")
        ),
        GreaterThanOrEqual("stringCol", "yyySmallerVal")
      ),
      And(
        LessThan("stringCol", "bbbPrefixFoo"),
        Or(
          StringStartsWith("stringCol", "bbbPrefix"),
          EqualTo("stringCol", "aaaBar")
        )
      )
    )
    val valuesInRangeSet: Array[String] = Array[String](
      "aaaBar",
      "bbbPrefix",
      "bbbPrefixFon",
      "wwwValLarger",
      "yyyVal",
      "zzz"
    )
    val valuesOutsideRangeSet: Array[String] = Array[String](
      "wwwVak",
      "wwwVal",
      "bbbNoPrefix",
      "bbbPrefixFoo",
      "bbbPrefixFop"
    )

    val resultRangeSet: RangeSet[RowKeyWrapper] =
      SparkSqlFilterAdapter.createRowKeyRangeSet(
        Array[Filter](filter),
        relation.catalog,
        pushDownRowKeyFilters = true
      )

    verifyInRangeSet(
      valuesInRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
    verifyOutsideRangeSet(
      valuesOutsideRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
  }

  // Unsupported filters (e.g., filters on non-row key columns and Not, etc.) are
  // converted into full range row keys. Should be converted to
  // ["cccSomePrefix", "cccSomePrefiy") \\union ("oooVal", +inf)
  test("stringRowKeyUnsupportedFilters") {
    val relation =
      BigtableRelation(createParametersMap(stringRowKeyCatalog), None)(
        sqlContext
      )
    val filter: Filter = Or(
      And(
        GreaterThan("stringCol", "oooVal"),
        // This will become (-inf, +inf)
        Not(GreaterThanOrEqual("stringCol", "aaaVal"))
      ),
      And(
        StringStartsWith("stringCol", "cccSomePrefix"),
        // This will become (-inf, +inf)
        LessThan("longCol", 1000L)
      )
    )
    val valuesInRangeSet: Array[String] = Array[String](
      "cccSomePrefix",
      "cccSomePrefix\u0000",
      "oooVal\u0000",
      "zzz"
    )
    val valuesOutsideRangeSet: Array[String] =
      Array[String]("", "cccSomethingElse", "cccSomePrefiy", "oooVal")

    val resultRangeSet: RangeSet[RowKeyWrapper] =
      SparkSqlFilterAdapter.createRowKeyRangeSet(
        Array[Filter](filter),
        relation.catalog,
        pushDownRowKeyFilters = true
      )

    verifyInRangeSet(
      valuesInRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
    verifyOutsideRangeSet(
      valuesOutsideRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
  }

  // Should cover the following long values:
  // (-300, -100) \\union {-50} \\union [-1, 100,000]. Considering byte array encoding, should be:
  // (-300, -100) \\union [-50, -50] \\union [-1, -1] \\union [0, 100,000]
  test("longRowKeyMixAndOr") {
    val relation =
      BigtableRelation(createParametersMap(longRowKeyCatalog), None)(sqlContext)
    val filter: Filter = Or(
      And(
        Or(
          LessThan("longCol", -100L),
          EqualTo("longCol", -50L)
        ),
        GreaterThan("longCol", -300L)
      ),
      And(
        Or(
          GreaterThanOrEqual("longCol", -1L),
          EqualTo("longCol", -75L)
        ),
        And(
          GreaterThanOrEqual("longCol", -10L),
          LessThanOrEqual("longCol", 100000L)
        )
      )
    )
    val valuesInRangeSet: Array[Long] =
      Array[Long](-299L, -101L, -50L, -1, 0L, 999L, 100000L)
    val valuesOutsideRangeSet: Array[Long] =
      Array[Long](-300L, -100L, -75L, -20L, 200000L)

    val resultRangeSet: RangeSet[RowKeyWrapper] =
      SparkSqlFilterAdapter.createRowKeyRangeSet(
        Array[Filter](filter),
        relation.catalog,
        pushDownRowKeyFilters = true
      )

    verifyInRangeSet(
      valuesInRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
    verifyOutsideRangeSet(
      valuesOutsideRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
  }

  // Considering the order of negative numbers in byte array (larger than positives) should become:
  // ([], [-20, 50]] \\union [[-20, -40, -100], [-20, -40, -100]].
  test("byteArrayRowKeyMixAndOr") {
    val relation =
      BigtableRelation(createParametersMap(byteArrayRowKeyCatalog), None)(
        sqlContext
      )
    val filter: Filter = And(
      Or(
        GreaterThan("binaryCol", Array[Byte]()),
        EqualTo("binaryCol", Array[Byte](-20, -40, -100))
      ),
      // Negative numbers are "larger" in binary format due to two's complement.
      And(
        LessThanOrEqual("binaryCol", Array[Byte](-20, -30, 40)),
        Or(
          LessThanOrEqual("binaryCol", Array[Byte](-20, 50)),
          EqualTo("binaryCol", Array[Byte](-20, -40, -100))
        )
      )
    )
    val valuesInRangeSet: Array[Array[Byte]] = Array[Array[Byte]](
      Array[Byte](0),
      Array[Byte](1, 2, 3),
      Array[Byte](-20, 15, 99),
      Array[Byte](-20, 50),
      Array[Byte](-20, -40, -100)
    )
    val valuesOutsideRangeSet: Array[Array[Byte]] = Array[Array[Byte]](
      Array[Byte](),
      Array[Byte](-20, 50, 0),
      Array[Byte](-20, -30),
      Array[Byte](-11, 0, -100)
    )

    val resultRangeSet: RangeSet[RowKeyWrapper] =
      SparkSqlFilterAdapter.createRowKeyRangeSet(
        Array[Filter](filter),
        relation.catalog,
        pushDownRowKeyFilters = true
      )

    verifyInRangeSet(valuesInRangeSet, resultRangeSet)
    verifyOutsideRangeSet(valuesOutsideRangeSet, resultRangeSet)
  }

  // Should become (-inf, +inf) since pushDownRowKeyFilters is false.
  test("pushDownFalse") {
    val relation =
      BigtableRelation(createParametersMap(stringRowKeyCatalog), None)(
        sqlContext
      )
    val filter: Filter = LessThanOrEqual("stringCol", "cccSomeValue")
    val valuesInRangeSet: Array[String] =
      Array[String]("aaaSmallerValue", "cccSomeValue", "zzzLargerValue")

    val resultRangeSet: RangeSet[RowKeyWrapper] =
      SparkSqlFilterAdapter.createRowKeyRangeSet(
        Array[Filter](filter),
        relation.catalog,
        pushDownRowKeyFilters = false
      )

    verifyInRangeSet(
      valuesInRangeSet.map(BytesConverter.toBytes),
      resultRangeSet
    )
  }

  def verifyInRangeSet(
      valueBytes: Array[Array[Byte]],
      rangeSet: RangeSet[RowKeyWrapper]
  ): Unit = {
    valueBytes.foreach(v => assert(rangeSet.contains(new RowKeyWrapper(v))))
  }

  def verifyOutsideRangeSet(
      valueBytes: Array[Array[Byte]],
      rangeSet: RangeSet[RowKeyWrapper]
  ): Unit = {
    valueBytes.foreach(v => assert(!rangeSet.contains(new RowKeyWrapper(v))))
  }

  def createParametersMap(catalog: String): Map[String, String] = {
    Map(
      "catalog" -> catalog,
      "spark.bigtable.project.id" -> "fake-project-id",
      "spark.bigtable.instance.id" -> "fake-instance-id"
    )
  }
}
