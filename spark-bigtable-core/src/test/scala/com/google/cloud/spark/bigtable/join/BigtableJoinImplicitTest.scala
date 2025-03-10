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
    val res5 = leftDf.as("a").joinWithBigtable(newJoinConfig2, "id", expr("a.id = b.id"))
    val res6 = leftDf.as("a").joinWithBigtable(newJoinConfig2, "id", col("a.id") === col("b.id"))
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
    val newJoinConfig = joinConfig ++ Map("join.type" -> "left")
    val res = leftDf.joinWithBigtable(newJoinConfig, "id")
    assert(leftDf.count == 3)
    assert(res.count == 3)
  }

  test("Left Anti Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig ++ Map("join.type" -> "left_anti")
    val res = leftDf.joinWithBigtable(newJoinConfig, "id")
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
      val res = leftDf.joinWithBigtable(newJoinConfig, "id")
      res.count()
    }
  }

  test("Right Join") {
    addSampleData()
    val joinConfig = createParametersMap(basicCatalog)
    val newJoinConfig = joinConfig ++ Map("join.type" -> "right")
    assertThrows[RuntimeException] {
      val res = leftDf.joinWithBigtable(newJoinConfig, "id")
      res.count()
    }
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
