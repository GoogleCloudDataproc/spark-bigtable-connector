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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.data.v2.models.Filters.FILTERS
import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.filters.{RowKeyWrapper, SparkSqlFilterAdapter}
import com.google.common.collect.RangeSet
import io.openlineage.spark.shade.client.OpenLineage
import io.openlineage.spark.shade.client.utils.DatasetIdentifier
import io.openlineage.spark.shade.extension.v1.{LineageRelation, LineageRelationProvider}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, Row => SparkRow}
import org.apache.yetus.audience.InterfaceAudience

object UserAgentInformation {
  val CONNECTOR_VERSION = "0.7.1" // ${NEXT_VERSION_FLAG}
  val DATA_SOURCE_VERSION = "V1"
  val DATAFRAME_TEXT = "DF/" + DATA_SOURCE_VERSION
  val RDD_TEXT = "RDD/"

  val DIRECT_JOINS_TEXT = "DIRECT_JOINS/"
  val scalaVersion = scala.util.Properties.versionNumberString

  // This remains unset only in unit tests where sqlContext is null.
  var sparkVersion = "UNSET_SPARK_VERSION"
}

/** Bigtable DefaultSource class which creates a BigtableRelation object.
  */
@InterfaceAudience.Private
class BigtableDefaultSource
    extends RelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with LineageRelationProvider {

  /** Constructs a BigtableRelation object.
    *
    * @param sqlContext Spark SQL context
    * @param parameters Parameters from Spark SQL
    * @return           A BigtableRelation object
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): BaseRelation = {
    new BigtableRelation(parameters, None)(sqlContext)
  }

  /** Constructs a BigtableRelation object after writing the provided data
    *   to Bigtable.
    *
    * @param sqlContext Spark SQL context
    * @param mode       The save mode
    * @param parameters Parameters from Spark SQL
    * @param data       The data to write to Bigtable
    * @return           A BigtableRelation object
    */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    val relation = BigtableRelation(parameters, Some(data.schema))(sqlContext)
    relation.createTableIfNeeded()
    relation.insert(data, overwrite = true)
    relation
  }

  override def shortName(): String = "bigtable"

  def getLineageDatasetIdentifier(
      sparkListenerEventName: String,
      openLineage: OpenLineage,
      sqlContext: Any,
      parameters: Any
  ): DatasetIdentifier = {
    val params: Map[String, String] =
      parameters.asInstanceOf[Map[String, String]]
    val catalog = BigtableTableCatalog(params)
    val projectId = params.getOrElse(
      BigtableSparkConf.BIGTABLE_PROJECT_ID,
      "unknownProjectId"
    )
    val instanceId = params.getOrElse(
      BigtableSparkConf.BIGTABLE_INSTANCE_ID,
      "unknownInstanceId"
    )
    new DatasetIdentifier(catalog.name, s"bigtable://$projectId/$instanceId")
  }
}

/** Custom Relation class which manages reading from and writing to Bigtable.
  *
  * @param sqlContext              Spark SQL context
  */
@InterfaceAudience.Private
case class BigtableRelation(
    @transient parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType]
)(@transient val sqlContext: SQLContext)
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with Logging
    with LineageRelation {

  val catalog: BigtableTableCatalog = BigtableTableCatalog(parameters)
  private val bigtableSparkConfBuilder =
    BigtableSparkConfBuilder()
      .fromMap(parameters)
      .setUserAgentSourceInfo(UserAgentInformation.DATAFRAME_TEXT)

  Option(sqlContext).foreach(context =>
    bigtableSparkConfBuilder.setSparkVersion(context.sparkContext.version)
  )
  val bigtableSparkConf: BigtableSparkConf = bigtableSparkConfBuilder.build()

  val pushDownRowKeyFilters: Boolean = bigtableSparkConf.appConfig.sparkScanConfig.pushDownRowKeyFilters

  // We get the timestamp in milliseconds but have to convert it to
  // microseconds before sending it to Bigtable.
  val startTimestampMicros: Option[Long] =
    bigtableSparkConf.appConfig.sparkScanConfig.timeRangeStart.map(timestamp => Math.multiplyExact(timestamp, 1000L))
  val endTimestampMicros: Option[Long] =
    bigtableSparkConf.appConfig.sparkScanConfig.timeRangeEnd.map(timestamp => Math.multiplyExact(timestamp, 1000L))

  val writeTimestampMicros: Long = bigtableSparkConf.appConfig.sparkWritesConfig.writeTimestamp
    .map(timestamp => Math.multiplyExact(timestamp, 1000L))
    .getOrElse(Math.multiplyExact(System.currentTimeMillis(), 1000L))

  def tableId = s"${catalog.name}"
  val clientKey = bigtableSparkConf.bigtableClientConfig

  override val schema: StructType =
    userSpecifiedSchema.getOrElse(catalog.toDataType)

  /** If specified by the user, create a new table
    *  (throw exception if a table with that name already exists).
    */
  def createTableIfNeeded(): Unit = {
    val createNewTable: Boolean = bigtableSparkConf.appConfig.sparkWritesConfig.createNewTable
    if (createNewTable) {
      val bigtableAdminClient: BigtableTableAdminClient =
        BigtableAdminClientBuilder.getAdminClient(clientKey)

      var createTableRequest: CreateTableRequest =
        CreateTableRequest.of(tableId)
      val columnFamilies = catalog.getColumnFamilies
      columnFamilies.foreach { columnFamily =>
        createTableRequest = createTableRequest.addFamily(columnFamily)
      }
      bigtableAdminClient.createTable(createTableRequest)
      bigtableAdminClient.close()
    }
  }

  /** Takes a Spark SQL DataFrame and converts and writes it to Bigtable.
    *
    * @param data      The input DataFrame
    * @param overwrite Whether to overwrite the existing data in Bigtable or not. This
    *                    is defined since the function is overridden. In reality,
    *                    we always overwrite the data due to Bigtable design.
    * @return          A map from fields in the row key to the their value
    */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val writeRowConversions =
      new WriteRowConversions(catalog, schema, writeTimestampMicros)
    data.rdd
      .map(writeRowConversions.convertToBigtableRowMutation)
      .foreachPartition(it => {
        if (it.nonEmpty) {
          val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
          val bigtableDataClient = clientHandle.getClient()
          val batcher = bigtableDataClient.newBulkMutationBatcher(tableId)
          it.foreach { mutation =>
            batcher.add(mutation)
          }
          batcher.flush()
          batcher.close()
          clientHandle.close()
        }
      })
  }

  /** Here we create the RDD[SparkRow] after reading from Bigtable.
    *
    * @param requiredColumns Columns requested by the query
    * @param filters         Filters to be applied
    * @return                Resulting RDD for Spark SQL
    */
  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter]
  ): RDD[SparkRow] = {
    val filterRangeSet: RangeSet[RowKeyWrapper] = SparkSqlFilterAdapter
      .createRowKeyRangeSet(filters, catalog, pushDownRowKeyFilters)

    val timestampFilter = (startTimestampMicros, endTimestampMicros) match {
      case (Some(startStamp), Some(endStamp)) =>
        FILTERS.timestamp().range().startClosed(startStamp).endOpen(endStamp))
      case (None, Some(endStamp)) =>
        FILTERS.timestamp().range().endOpen(endStamp))
      case (Some(startStamp), None) =>
        FILTERS.timestamp().range().startClosed(startStamp))
      case (None, None) => // No timestamp filter
    }

    val filter = FILTERS.chain().filter(timestampFilter)
      .filter(FILTERS.limit().cellsPerColumn(1))

    val readRdd: BigtableTableScanRDD =
      new BigtableTableScanRDD(
        clientKey,
        filterRangeSet,
        tableId,
        sqlContext.sparkContext,
        filter
      )

    val fieldsOrdered = requiredColumns.map(catalog.sMap.getField)
    readRdd.flatMap(r => ReadRowConversions.buildRow(fieldsOrdered, r, catalog))
  }

  def getLineageDatasetIdentifier(
      sparkListenerEventName: String,
      openLineage: OpenLineage
  ): DatasetIdentifier = {
    val projectId = parameters.getOrElse(
      BigtableSparkConf.BIGTABLE_PROJECT_ID,
      "unknownProjectId"
    )
    val instanceId = parameters.getOrElse(
      BigtableSparkConf.BIGTABLE_INSTANCE_ID,
      "unknownInstanceId"
    )
    new DatasetIdentifier(tableId, s"bigtable://$projectId/$instanceId")
  }
}
