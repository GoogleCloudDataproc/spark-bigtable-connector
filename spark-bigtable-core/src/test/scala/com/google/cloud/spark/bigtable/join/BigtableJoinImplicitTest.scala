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

package com.google.cloud.spark.bigtable.join

import com.google.bigtable.v2.SampleRowKeysResponse
import com.google.cloud.spark.bigtable.Logging
import com.google.cloud.spark.bigtable.fakeserver.{
  FakeCustomDataService,
  FakeGenericDataService,
  FakeServerBuilder
}
import com.google.protobuf.ByteString
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Random

class BigtableJoinImplicitTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {
  @transient lazy implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("BigtableJoinTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  var leftDf: DataFrame = _

  val basicCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"stringCol", "type":"string"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"}
       |}
       |}""".stripMargin

  val basicCatalogWithDynamicColumns: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol",
       |"columns":{
       |"id":{"cf":"rowkey", "col":"stringCol", "type":"string"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"}
       |},
       |"regexColumns":{
       |"dyn_col1":{"cf":"cf1", "pattern":"co.*", "type":"string"}
       |}
       |}""".stripMargin

  var fakeCustomDataService: FakeCustomDataService = _
  var fakeGenericDataService: FakeGenericDataService = _
  var emulatorPort: String = _

  override def beforeAll(): Unit = {
    leftDf = Seq(
      ("row1", "value1"),
      ("row2", "value2"),
      ("row3", "value3")
    ).toDF("id", "col1")
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def beforeEach(): Unit = {
    fakeCustomDataService = new FakeCustomDataService
    val server = new FakeServerBuilder()
      .addService(fakeCustomDataService)
      .start
    emulatorPort = Integer.toString(server.getPort)
    logInfo("Bigtable mock server started on port " + emulatorPort)
  }

  import com.google.cloud.spark.bigtable.join.BigtableJoinImplicit._

  test("Inner Join With Bigtable Table") {
    addSampleData()
    val leftCount = leftDf.count
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig1 = joinConfig ++ Map("join.type" -> "inner")
    val newJoinConfig2 = newJoinConfig1 ++ Map("alias.name" -> "b")
    val res1 = leftDf.joinWithBigtable(newJoinConfig1, "id")
    val res2 = leftDf.joinWithBigtable(newJoinConfig1, "id", Seq("id"))
    val res3 = leftDf.joinWithBigtable(newJoinConfig1, "id", Array("id"))
    val res4 = leftDf.joinWithBigtable(newJoinConfig1, "id", List("id"))
    val res5 =
      leftDf.as("a").joinWithBigtable(newJoinConfig2, "id", expr("a.id = b.id"), aliasName = "b")
    val res6 = leftDf
      .as("a")
      .joinWithBigtable(newJoinConfig2, "id", col("a.id") === col("b.id"), aliasName = "b")
    assert(res1.count == leftCount)
    assert(res2.count == leftCount)
    assert(res3.count == leftCount)
    assert(res4.count == leftCount)
    assert(res5.count == leftCount)
    assert(res6.count == leftCount)
  }

  test("Left Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig
    val res = leftDf.joinWithBigtable(newJoinConfig, "id", joinType = "left")
    assert(leftDf.count == 3)
    assert(res.count == 3)
  }

  test("Left Anti Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig
    val res = leftDf.joinWithBigtable(newJoinConfig, "id", joinType = "left_anti")
    assert(leftDf.count == 3)
    assert(res.count == 0)
  }

  test("Left Semi Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig ++ Map("join.type" -> "left_semi")
    val res = leftDf.joinWithBigtable(newJoinConfig, "id")
    assert(leftDf.count == 3)
    assert(res.count == 3)
  }

  test("Full Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig ++ Map("join.type" -> "full")
    assertThrows[RuntimeException] {
      val res = leftDf.joinWithBigtable(newJoinConfig, "id", joinType = "full")
      res.count()
    }
  }

  test("Right Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig
    assertThrows[RuntimeException] {
      val res = leftDf.joinWithBigtable(newJoinConfig, "id", joinType = "right")
      res.count()
    }
  }

  test("Join with dynamic column") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalogWithDynamicColumns)
    val newJoinConfig = joinConfig
    val res = leftDf.joinWithBigtable(newJoinConfig, "id")
    assert(res.columns.contains("dyn_col1"))
  }

  test("Join with missing rows in Bigtable") {
    // Add data for row1 and row3 in the correct column family
    fakeCustomDataService.addRow("row1", "cf1", "col1", "bt_value1")
    fakeCustomDataService.addRow("row3", "cf1", "col1", "bt_value3")
    // Add the "missing" row but with a different, irrelevant column family.
    // This ensures the row key exists, avoiding the fake server crash,
    // but it will have no data for the columns requested in the join.
    fakeCustomDataService.addRow("row_missing", "cf2", "col_other", "some_other_value")

    val leftDfWithMissingRow = Seq(
      ("row1", "value1_left"),
      ("row_missing", "value_missing_left"),
      ("row3", "value3_left")
    ).toDF("id", "left_col1")

    val joinConfig = createParametersMap(basicCatalog)
    val resultDf =
      leftDfWithMissingRow.joinWithBigtable(joinConfig, "id", joinType = "left")

    val expectedData = Seq(
      ("row1", "value1_left", "bt_value1"),
      ("row_missing", "value_missing_left", null),
      ("row3", "value3_left", "bt_value3")
    )
    val expectedDf = expectedData.toDF("id", "left_col1", "col1")

    // The code should not crash and the result should match the expected DataFrame.
    assert(resultDf.count() == expectedDf.count())
    assert(resultDf.except(expectedDf).count() == 0)
    assert(expectedDf.except(resultDf).count() == 0)
  }

  def createSampleRowKeyResponse(rowKey: Array[Byte]): SampleRowKeysResponse = {
    SampleRowKeysResponse
      .newBuilder()
      .setRowKey(ByteString.copyFrom(rowKey))
      .build()
  }

  def createParametersMap(catalog: String): Map[String, String] = {
    Map(
      "catalog" -> catalog,
      "spark.bigtable.project.id" -> "fake-project-id",
      "spark.bigtable.instance.id" -> "fake-instance-id",
      "spark.bigtable.emulator.port" -> emulatorPort
    )
  }

  def addSampleData(): Unit = {
    1 to 9 map (key => s"row$key") foreach { key =>
      fakeCustomDataService.addRow(
        key,
        "cf1",
        "col1",
        Random.nextString(10)
      )
    }
  }
}
