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

package com.google.cloud.spark.bigtable

import com.google.api.gax.core.{CredentialsProvider, NoCredentialsProvider}
import com.google.auth.Credentials
import com.google.bigtable.v2.SampleRowKeysResponse
import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.fakeserver.{FakeCustomDataService, FakeServerBuilder}
import com.google.cloud.spark.bigtable.filters.RowKeyWrapper
import com.google.common.collect.{Range, RangeSet, TreeRangeSet}
import com.google.protobuf.ByteString
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class BigtableTableScanRDDTest
    extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {
  // These tests only use the catalog to initialize the class, other variables are passed
  // to them as arguments directly. We therefore use a dummy catalog.
  val basicCatalog: String =
    s"""{
       |"table":{"name":"tableName"},
       |"rowkey":"stringCol",
       |"columns":{
       |"stringCol":{"cf":"rowkey", "col":"stringCol", "type":"string"}
       |}
       |}""".stripMargin

  var fakeCustomDataService: FakeCustomDataService = _
  var emulatorPort: String = _
  @transient var sc: SparkContext = _
  var sqlContext: SQLContext = _
  var rdd: BigtableTableScanRDD = _

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf
    sc = new SparkContext("local", "test", sparkConf)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  override def beforeEach(): Unit = {
    fakeCustomDataService = new FakeCustomDataService
    val server = new FakeServerBuilder().addService(fakeCustomDataService).start
    emulatorPort = Integer.toString(server.getPort)
    logInfo("Bigtable mock server started on port " + emulatorPort)
    ParamsTracker.setParams.clear()
  }

  test("simpleStringBoundaries") {
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("ccc_1"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("hhh_2"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(ByteString.EMPTY.toByteArray)
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("aaa_partition0"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition1"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("eee_partition1"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("zzz_partition2"))
      )
    )
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    val actualPartitions: Array[BigtableScanPartition] =
      rdd.getPartitions.map(partition => partition.asInstanceOf[BigtableScanPartition])
    assert(actualPartitions.length == 3)

    val expectedSetResult0: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult0.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("aaa_partition0"))
      )
    )
    assert(actualPartitions(0).partitionRangeSet.equals(expectedSetResult0))

    val expectedSetResult1: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult1.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition1"))
      )
    )
    expectedSetResult1.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("eee_partition1"))
      )
    )
    assert(actualPartitions(1).partitionRangeSet.equals(expectedSetResult1))

    val expectedSetResult2: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult2.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("zzz_partition2"))
      )
    )
    assert(actualPartitions(2).partitionRangeSet.equals(expectedSetResult2))
  }

  // Our partitioning should work regardless of whether the last
  // SampleRowKey response has an empty row key or not.
  test("nonEmptyFinalTablet") {
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("aaa_1"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("hhh_final_tablet"))
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("bbb_partition0"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ccc_partition0"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("mmm_partition0"))
      )
    )
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    val actualPartitions: Array[BigtableScanPartition] =
      rdd.getPartitions.map(partition => partition.asInstanceOf[BigtableScanPartition])

    assert(actualPartitions.length == 1)

    val expectedSetResult: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("bbb_partition0"))
      )
    )
    expectedSetResult.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ccc_partition0"))
      )
    )
    expectedSetResult.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("mmm_partition0"))
      )
    )
    assert(actualPartitions(0).partitionRangeSet.equals(expectedSetResult))
  }

  test("rangesAndPoints") {
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("aaa_1"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("hhh_2"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(ByteString.EMPTY.toByteArray)
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    filterRangeSet.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("bbb_partition0")),
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition0"))
      )
    )
    filterRangeSet.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("fff_partition0")),
        new RowKeyWrapper(BytesConverter.toBytes("ggg_partition0"))
      )
    )
    filterRangeSet.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("mmm_partition1")),
        new RowKeyWrapper(BytesConverter.toBytes("nnn_partition1"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ccc_partition0"))
      )
    )
    filterRangeSet.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition0"))
      )
    )
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    val actualPartitions: Array[BigtableScanPartition] =
      rdd.getPartitions.map(partition => partition.asInstanceOf[BigtableScanPartition])
    assert(actualPartitions.length == 2)

    val expectedSetResult0: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult0.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("bbb_partition0")),
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition0"))
      )
    )
    expectedSetResult0.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("fff_partition0")),
        new RowKeyWrapper(BytesConverter.toBytes("ggg_partition0"))
      )
    )
    expectedSetResult0.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ccc_partition0"))
      )
    )
    expectedSetResult0.add(
      Range.singleton(
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition0"))
      )
    )
    assert(actualPartitions(0).partitionRangeSet.equals(expectedSetResult0))

    val expectedSetResult1: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult1.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("mmm_partition1")),
        new RowKeyWrapper(BytesConverter.toBytes("nnn_partition1"))
      )
    )
    assert(actualPartitions(1).partitionRangeSet.equals(expectedSetResult1))
  }

  test("tabletBoundaryIntersection") {
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("ccc_1"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("hhh_2"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("mmm_3"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("qqq_4"))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(ByteString.EMPTY.toByteArray)
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    filterRangeSet.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("bbb_partition0/1")),
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition0/1"))
      )
    )
    filterRangeSet.add(
      Range.singleton(new RowKeyWrapper(BytesConverter.toBytes("qqq_4")))
    )
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    val actualPartitions: Array[BigtableScanPartition] =
      rdd.getPartitions.map(partition => partition.asInstanceOf[BigtableScanPartition])
    assert(actualPartitions.length == 3)

    val expectedSetResult0: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult0.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("bbb_partition0/1")),
        new RowKeyWrapper(BytesConverter.toBytes("ccc_1"))
      )
    )
    assert(actualPartitions(0).partitionRangeSet.equals(expectedSetResult0))

    val expectedSetResult1: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult1.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes("ccc_1")),
        new RowKeyWrapper(BytesConverter.toBytes("ddd_partition0/1"))
      )
    )
    assert(actualPartitions(1).partitionRangeSet.equals(expectedSetResult1))

    val expectedSetResult2: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult2.add(
      Range.singleton(new RowKeyWrapper(BytesConverter.toBytes("qqq_4")))
    )
    assert(actualPartitions(2).partitionRangeSet.equals(expectedSetResult2))
  }

  test("tabletBoundaryIntersectionWithLong") {
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(500L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(1000L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(1500L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(2000L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(2500L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(ByteString.EMPTY.toByteArray)
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    // Partitions 0 and 1, i.e., [1000, 1500) and [1500, 2000)
    filterRangeSet.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes(1000L)),
        new RowKeyWrapper(BytesConverter.toBytes(2000L))
      )
    )
    // Partition 2, i.e., [2500, +inf)
    filterRangeSet.add(
      Range.singleton(new RowKeyWrapper(BytesConverter.toBytes(2500L)))
    )
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    val actualPartitions: Array[BigtableScanPartition] =
      rdd.getPartitions.map(partition => partition.asInstanceOf[BigtableScanPartition])
    assert(actualPartitions.length == 3)

    val expectedSetResult0: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult0.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes(1000L)),
        new RowKeyWrapper(BytesConverter.toBytes(1500L))
      )
    )
    assert(actualPartitions(0).partitionRangeSet.equals(expectedSetResult0))

    val expectedSetResult1: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult1.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes(1500L)),
        new RowKeyWrapper(BytesConverter.toBytes(2000L))
      )
    )
    assert(actualPartitions(1).partitionRangeSet.equals(expectedSetResult1))

    val expectedSetResult2: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult2.add(
      Range.singleton(new RowKeyWrapper(BytesConverter.toBytes(2500L)))
    )
    assert(actualPartitions(2).partitionRangeSet.equals(expectedSetResult2))
  }

  test("tabletSingleRangeBoundaryIntersectionWithLong") {
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(1000L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(1500L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes(2000L))
    )
    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(ByteString.EMPTY.toByteArray)
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    // Partitions 0 and 1, i.e., [1600, 2000) and [2000, 2000]
    filterRangeSet.add(
      Range.closed(
        new RowKeyWrapper(BytesConverter.toBytes(1600L)),
        new RowKeyWrapper(BytesConverter.toBytes(2000L))
      )
    )
    val relation =
      BigtableRelation(createParametersMap(basicCatalog), None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    val actualPartitions: Array[BigtableScanPartition] =
      rdd.getPartitions.map(partition => partition.asInstanceOf[BigtableScanPartition])
    assert(actualPartitions.length == 2)

    val expectedSetResult0: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult0.add(
      Range.closedOpen(
        new RowKeyWrapper(BytesConverter.toBytes(1600L)),
        new RowKeyWrapper(BytesConverter.toBytes(2000L))
      )
    )
    assert(actualPartitions(0).partitionRangeSet.equals(expectedSetResult0))

    val expectedSetResult1: RangeSet[RowKeyWrapper] =
      TreeRangeSet.create[RowKeyWrapper]()
    expectedSetResult1.add(
      Range.singleton(new RowKeyWrapper(BytesConverter.toBytes(2000L)))
    )
    assert(actualPartitions(1).partitionRangeSet.equals(expectedSetResult1))
  }

  test("Properly pass custom authentication parameters to scan") {
    val parametersMap = createParametersMap(basicCatalog) + (
      "spark.bigtable.auth.credentials_provider" -> "com.google.cloud.spark.bigtable.CustomAuthProvider",
      "spark.bigtable.auth.credentials_provider.args.param1" -> "expected",
      "spark.bigtable.auth.credentials_provider.args.param2" -> "expected2"
    )

    fakeCustomDataService.addSampleRowKeyResponse(
      createSampleRowKeyResponse(BytesConverter.toBytes("ccc_1"))
    )

    val filterRangeSet: RangeSet[RowKeyWrapper] = TreeRangeSet.create[RowKeyWrapper]()

    val relation =
      BigtableRelation(parametersMap, None)(sqlContext)
    rdd = new BigtableTableScanRDD(
      relation.clientKey,
      filterRangeSet,
      relation.tableId,
      relation.sqlContext.sparkContext,
      relation.startTimestampMicros,
      relation.endTimestampMicros,
      None
    )

    // All we want is to validate our class was instantiated
    val _ = rdd.getPartitions

    assert(ParamsTracker.setParams == Set(Map("spark.bigtable.auth.credentials_provider.args.param1" -> "expected", "spark.bigtable.auth.credentials_provider.args.param2" -> "expected2")))
  }

  def createParametersMap(catalog: String): Map[String, String] = {
    Map(
      "catalog" -> catalog,
      "spark.bigtable.project.id" -> "fake-project-id",
      "spark.bigtable.instance.id" -> "fake-instance-id",
      "spark.bigtable.emulator.port" -> emulatorPort
    )
  }

  def createSampleRowKeyResponse(rowKey: Array[Byte]): SampleRowKeysResponse = {
    SampleRowKeysResponse
      .newBuilder()
      .setRowKey(ByteString.copyFrom(rowKey))
      .build()
  }
}

object ParamsTracker {
  val setParams: scala.collection.mutable.Set[Map[String, String]] = scala.collection.mutable.Set[Map[String, String]]()
}

class CustomAuthProvider(val params: Map[String, String]) extends CredentialsProvider {
  ParamsTracker.setParams += params
  private val proxyProvider = NoCredentialsProvider.create()

  override def getCredentials: Credentials = proxyProvider.getCredentials
}
