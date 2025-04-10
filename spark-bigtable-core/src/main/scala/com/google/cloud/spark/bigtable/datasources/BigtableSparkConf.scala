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

package com.google.cloud.spark.bigtable.datasources

import org.apache.yetus.audience.InterfaceAudience;

/** This is the bigtable configuration. Users can either set them in SparkConf, which
  * will take effect globally, or configure it per table, which will overwrite the value
  * set in SparkConf. If not set, the default value will take effect.
  * To access these variables inside Java, use the following import statement:
  * `import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.*;`
  * You can then use a syntax like `BIGTABLE_PROJECT_ID()` to access them.
  * When using RDDs, you can use a config builder object and config setter methods,
  * e.g., `new BigtableSparkConfBuilder().setProjectId(someProjectId).build()`
  * to create the `BigtableSparkConf` object with specific configs.
  */
@InterfaceAudience.Public
object BigtableSparkConf {

  /** The Bigtable project id. */
  val BIGTABLE_PROJECT_ID = "spark.bigtable.project.id"

  /** The Bigtable instance id. */
  val BIGTABLE_INSTANCE_ID = "spark.bigtable.instance.id"

  /** The Bigtable app profile. */
  val BIGTABLE_APP_PROFILE_ID = "spark.bigtable.app_profile.id"
  val DEFAULT_BIGTABLE_APP_PROFILE_ID = "default"

  /** Specifying whether to create a new Bigtable table or not. */
  val BIGTABLE_CREATE_NEW_TABLE = "spark.bigtable.create.new.table"
  val DEFAULT_BIGTABLE_CREATE_NEW_TABLE = false

  /** The timestamp start range to filter cell values. */
  val BIGTABLE_TIMERANGE_START =
    "spark.bigtable.read.timerange.start.milliseconds"

  /** The timestamp end range to filter cell values. */
  val BIGTABLE_TIMERANGE_END = "spark.bigtable.read.timerange.end.milliseconds"

  /** The timestamp to set on all cells when writing. If not specified,
    *    all cells will have the current timestamp.
    */
  val BIGTABLE_WRITE_TIMESTAMP = "spark.bigtable.write.timestamp.milliseconds"

  /** The emulator port. */
  val BIGTABLE_EMULATOR_PORT = "spark.bigtable.emulator.port"

  /** Specifying whether to push row key filters to Bigtable or not. */
  val BIGTABLE_PUSH_DOWN_ROW_KEY_FILTERS =
    "spark.bigtable.push.down.row.key.filters"

  /** By default, push down all filters (row key, column value, etc.) down. */
  val DEFAULT_BIGTABLE_PUSH_DOWN_FILTERS = true

  /** The timeout duration for read rows request attempts. */
  val BIGTABLE_READ_ROWS_ATTEMPT_TIMEOUT_MS =
    "spark.bigtable.read.rows.attempt.timeout.milliseconds"

  /** The total timeout duration for read rows request. */
  val BIGTABLE_READ_ROWS_TOTAL_TIMEOUT_MS =
    "spark.bigtable.read.rows.total.timeout.milliseconds"

  /** The timeout duration for bulk mutation request attempts. */
  val BIGTABLE_MUTATE_ROWS_ATTEMPT_TIMEOUT_MS =
    "spark.bigtable.mutate.rows.attempt.timeout.milliseconds"

  /** The total timeout duration for bulk mutation requests. */
  val BIGTABLE_MUTATE_ROWS_TOTAL_TIMEOUT_MS =
    "spark.bigtable.mutate.rows.total.timeout.milliseconds"

  /** The batch size for batch mutation requests. Maximum is 100K. */
  val BIGTABLE_BATCH_MUTATE_SIZE = "spark.bigtable.batch.mutate.size"
  val BIGTABLE_DEFAULT_BATCH_MUTATE_SIZE = 100
  val BIGTABLE_MAX_BATCH_MUTATE_SIZE = 100000

  /** Signals if batch mutations should use flow control. */
  val BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL =
    "spark.bigtable.enable.batch_mutate.flow_control"
  val DEFAULT_BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL = false

  val BIGTABLE_CUSTOM_ACCESS_TOKEN_PROVIDER = "spark.bigtable.auth.access_token_provider"

  /** Used for internal testing and not officially supported. */
  private[datasources] val MAX_READ_ROWS_RETRIES =
    "spark.bigtable.max.read.rows.retries"
}

class BigtableSparkConfBuilder extends Serializable {
  private var projectId: Option[String] = None
  private var instanceId: Option[String] = None
  private var appProfileId: String = BigtableSparkConf.DEFAULT_BIGTABLE_APP_PROFILE_ID

  private var createNewTable: Boolean = BigtableSparkConf.DEFAULT_BIGTABLE_CREATE_NEW_TABLE

  private var timeRangeStart: Option[Long] = None
  private var timeRangeEnd: Option[Long] = None
  private var writeTimestamp: Option[Long] = None

  private var emulatorPort: Option[Int] = None

  private var pushDownRowKeyFilters: Boolean =
    BigtableSparkConf.DEFAULT_BIGTABLE_PUSH_DOWN_FILTERS

  private var readRowsAttemptTimeoutMs: Option[String] = None
  private var readRowsTotalTimeoutMs: Option[String] = None

  private var mutateRowsAttemptTimeoutMs: Option[String] = None
  private var mutateRowsTotalTimeoutMs: Option[String] = None

  private var batchMutateSize: Long = BigtableSparkConf.BIGTABLE_DEFAULT_BATCH_MUTATE_SIZE
  private var enableBatchMutateFlowControl: Boolean =
    BigtableSparkConf.DEFAULT_BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL

  private var maxReadRowsRetries: Option[String] = None

  private var customAccessTokenProviderFQCN: Option[String] = None

  // This function is package-private to allow internal setup when using DataFrames
  // (since Spark SQL passes a Map<String, String> object). However, for external use with RDDs,
  // users need to use the setter methods to set the configs.
  private[bigtable] def fromMap(
                                 conf: Map[String, String],
                                 globalConfig: Map[String, String]
                               ): BigtableSparkConfBuilder = {
    this.projectId = conf.get(BigtableSparkConf.BIGTABLE_PROJECT_ID)
    this.instanceId = conf.get(BigtableSparkConf.BIGTABLE_INSTANCE_ID)
    this.appProfileId = conf.getOrElse(
      BigtableSparkConf.BIGTABLE_APP_PROFILE_ID,
      BigtableSparkConf.DEFAULT_BIGTABLE_APP_PROFILE_ID
    )

    this.createNewTable = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_CREATE_NEW_TABLE,
        BigtableSparkConf.DEFAULT_BIGTABLE_CREATE_NEW_TABLE.toString
      )
      .toBoolean

    this.timeRangeStart = conf.get(BigtableSparkConf.BIGTABLE_TIMERANGE_START).map(_.toLong)
    this.timeRangeEnd = conf.get(BigtableSparkConf.BIGTABLE_TIMERANGE_END).map(_.toLong)
    this.writeTimestamp = conf.get(BigtableSparkConf.BIGTABLE_WRITE_TIMESTAMP).map(_.toLong)

    this.emulatorPort = conf.get(BigtableSparkConf.BIGTABLE_EMULATOR_PORT).map(_.toInt)

    this.pushDownRowKeyFilters = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_PUSH_DOWN_ROW_KEY_FILTERS,
        BigtableSparkConf.DEFAULT_BIGTABLE_PUSH_DOWN_FILTERS.toString
      )
      .toBoolean

    this.readRowsAttemptTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_READ_ROWS_ATTEMPT_TIMEOUT_MS)
    this.readRowsTotalTimeoutMs = conf.get(BigtableSparkConf.BIGTABLE_READ_ROWS_TOTAL_TIMEOUT_MS)

    this.mutateRowsAttemptTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_MUTATE_ROWS_ATTEMPT_TIMEOUT_MS)
    this.mutateRowsTotalTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_MUTATE_ROWS_TOTAL_TIMEOUT_MS)

    this.batchMutateSize = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_BATCH_MUTATE_SIZE,
        BigtableSparkConf.BIGTABLE_DEFAULT_BATCH_MUTATE_SIZE.toString
      )
      .toLong
    this.enableBatchMutateFlowControl = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL,
        BigtableSparkConf.DEFAULT_BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL.toString
      )
      .toBoolean

    this.maxReadRowsRetries = conf.get(BigtableSparkConf.MAX_READ_ROWS_RETRIES)

    this.customAccessTokenProviderFQCN = conf
      .get(BigtableSparkConf.BIGTABLE_CUSTOM_ACCESS_TOKEN_PROVIDER)
      .orElse(globalConfig.get(BigtableSparkConf.BIGTABLE_CUSTOM_ACCESS_TOKEN_PROVIDER))

    this
  }

  def setProjectId(value: String): BigtableSparkConfBuilder = {
    projectId = Some(value)
    this
  }

  def setInstanceId(value: String): BigtableSparkConfBuilder = {
    instanceId = Some(value)
    this
  }

  def setAppProfileId(value: String): BigtableSparkConfBuilder = {
    appProfileId = value
    this
  }

  def setReadRowsAttemptTimeoutMs(value: String): BigtableSparkConfBuilder = {
    readRowsAttemptTimeoutMs = Some(value)
    this
  }

  def setReadRowsTotalTimeoutMs(value: String): BigtableSparkConfBuilder = {
    readRowsTotalTimeoutMs = Some(value)
    this
  }

  def setMutateRowsAttemptTimeoutMs(value: String): BigtableSparkConfBuilder = {
    mutateRowsAttemptTimeoutMs = Some(value)
    this
  }

  def setMutateRowsTotalTimeoutMs(value: String): BigtableSparkConfBuilder = {
    mutateRowsTotalTimeoutMs = Some(value)
    this
  }

  def setBatchMutateSize(value: Int): BigtableSparkConfBuilder = {
    batchMutateSize = value
    this
  }

  def setEnableBatchMutateFlowControl(value: Boolean): BigtableSparkConfBuilder = {
    enableBatchMutateFlowControl = value
    this
  }

  def build(): BigtableSparkConf = {
    new BigtableSparkConf(
      projectId,
      instanceId,
      appProfileId,
      createNewTable,
      timeRangeStart,
      timeRangeEnd,
      writeTimestamp,
      emulatorPort,
      pushDownRowKeyFilters,
      readRowsAttemptTimeoutMs,
      readRowsTotalTimeoutMs,
      mutateRowsAttemptTimeoutMs,
      mutateRowsTotalTimeoutMs,
      batchMutateSize,
      enableBatchMutateFlowControl,
      maxReadRowsRetries,
      customAccessTokenProviderFQCN
    )
  }
}

object BigtableSparkConfBuilder {
  def apply(): BigtableSparkConfBuilder = new BigtableSparkConfBuilder()
}

class BigtableSparkConf private[datasources] (
    val projectId: Option[String],
    val instanceId: Option[String],
    val appProfileId: String,
    val createNewTable: Boolean,
    val timeRangeStart: Option[Long],
    val timeRangeEnd: Option[Long],
    val writeTimestamp: Option[Long],
    val emulatorPort: Option[Int],
    val pushDownRowKeyFilters: Boolean,
    val readRowsAttemptTimeoutMs: Option[String],
    val readRowsTotalTimeoutMs: Option[String],
    val mutateRowsAttemptTimeoutMs: Option[String],
    val mutateRowsTotalTimeoutMs: Option[String],
    val batchMutateSize: Long,
    val enableBatchMutateFlowControl: Boolean,
    val maxReadRowsRetries: Option[String],
    val customAccessTokenProviderFQCN: Option[String]
) extends Serializable
