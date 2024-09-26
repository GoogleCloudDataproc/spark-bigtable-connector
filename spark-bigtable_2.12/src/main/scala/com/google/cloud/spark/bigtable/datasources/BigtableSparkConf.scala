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
  * When using RDDs, you can either provide a Map of configs/values or use the
  * default constructor `new BigtableSparkConf()` and use methods such as
  * `setBigtableProjectId` to set specific values directly.
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

  /** Used for internal testing and not officially supported. */
  private[datasources] val MAX_READ_ROWS_RETRIES =
    "spark.bigtable.max.read.rows.retries"
}

class BigtableSparkConfBuilder extends Serializable {
  private var bigtableProjectId: Option[String] = None
  private var bigtableInstanceId: Option[String] = None
  private var bigtableAppProfileId: String = BigtableSparkConf.DEFAULT_BIGTABLE_APP_PROFILE_ID

  private var bigtableCreateNewTable: Boolean = BigtableSparkConf.DEFAULT_BIGTABLE_CREATE_NEW_TABLE

  private var bigtableTimeRangeStart: Option[Long] = None
  private var bigtableTimeRangeEnd: Option[Long] = None
  private var bigtableWriteTimestamp: Option[Long] = None

  private var bigtableEmulatorPort: Option[Int] = None

  private var bigtablePushDownRowKeyFilters: Boolean =
    BigtableSparkConf.DEFAULT_BIGTABLE_PUSH_DOWN_FILTERS

  private var bigtableReadRowsAttemptTimeoutMs: Option[String] = None
  private var bigtableReadRowsTotalTimeoutMs: Option[String] = None

  private var bigtableMutateRowsAttemptTimeoutMs: Option[String] = None
  private var bigtableMutateRowsTotalTimeoutMs: Option[String] = None

  private var bigtableBatchMutateSize: Long = BigtableSparkConf.BIGTABLE_DEFAULT_BATCH_MUTATE_SIZE
  private var bigtableEnableBatchMutateFlowControl: Boolean =
    BigtableSparkConf.DEFAULT_BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL

  private var maxReadRowsRetries: Option[String] = None

  // This function is package-private to allow internal setup when using DataFrames
  // (since Spark SQL passes a Map<String, String> object). However, for external use with RDDs,
  // users need to use the setter methods to set the configs.
  private[bigtable] def fromMap(conf: Map[String, String]): BigtableSparkConfBuilder = {
    this.bigtableProjectId = conf.get(BigtableSparkConf.BIGTABLE_PROJECT_ID)
    this.bigtableInstanceId = conf.get(BigtableSparkConf.BIGTABLE_INSTANCE_ID)
    this.bigtableAppProfileId = conf.getOrElse(
      BigtableSparkConf.BIGTABLE_APP_PROFILE_ID,
      BigtableSparkConf.DEFAULT_BIGTABLE_APP_PROFILE_ID
    )

    this.bigtableCreateNewTable = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_CREATE_NEW_TABLE,
        BigtableSparkConf.DEFAULT_BIGTABLE_CREATE_NEW_TABLE.toString
      )
      .toBoolean

    this.bigtableTimeRangeStart = conf.get(BigtableSparkConf.BIGTABLE_TIMERANGE_START).map(_.toLong)
    this.bigtableTimeRangeEnd = conf.get(BigtableSparkConf.BIGTABLE_TIMERANGE_END).map(_.toLong)
    this.bigtableWriteTimestamp = conf.get(BigtableSparkConf.BIGTABLE_WRITE_TIMESTAMP).map(_.toLong)

    this.bigtableEmulatorPort = conf.get(BigtableSparkConf.BIGTABLE_EMULATOR_PORT).map(_.toInt)

    this.bigtablePushDownRowKeyFilters = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_PUSH_DOWN_ROW_KEY_FILTERS,
        BigtableSparkConf.DEFAULT_BIGTABLE_PUSH_DOWN_FILTERS.toString
      )
      .toBoolean

    this.bigtableReadRowsAttemptTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_READ_ROWS_ATTEMPT_TIMEOUT_MS)
    this.bigtableReadRowsTotalTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_READ_ROWS_TOTAL_TIMEOUT_MS)

    this.bigtableMutateRowsAttemptTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_MUTATE_ROWS_ATTEMPT_TIMEOUT_MS)
    this.bigtableMutateRowsTotalTimeoutMs =
      conf.get(BigtableSparkConf.BIGTABLE_MUTATE_ROWS_TOTAL_TIMEOUT_MS)

    this.bigtableBatchMutateSize = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_BATCH_MUTATE_SIZE,
        BigtableSparkConf.BIGTABLE_DEFAULT_BATCH_MUTATE_SIZE.toString
      )
      .toLong
    this.bigtableEnableBatchMutateFlowControl = conf
      .getOrElse(
        BigtableSparkConf.BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL,
        BigtableSparkConf.DEFAULT_BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL.toString
      )
      .toBoolean

    this.maxReadRowsRetries = conf.get(BigtableSparkConf.MAX_READ_ROWS_RETRIES)

    this
  }

  def setBigtableProjectId(value: String): BigtableSparkConfBuilder = {
    bigtableProjectId = Some(value)
    this
  }

  def setBigtableInstanceId(value: String): BigtableSparkConfBuilder = {
    bigtableInstanceId = Some(value)
    this
  }

  def setBigtableAppProfileId(value: String): BigtableSparkConfBuilder = {
    bigtableAppProfileId = value
    this
  }

  def setBigtableReadRowsAttemptTimeoutMs(value: String): BigtableSparkConfBuilder = {
    bigtableReadRowsAttemptTimeoutMs = Some(value)
    this
  }

  def setBigtableReadRowsTotalTimeoutMs(value: String): BigtableSparkConfBuilder = {
    bigtableReadRowsTotalTimeoutMs = Some(value)
    this
  }

  def setBigtableMutateRowsAttemptTimeoutMs(value: String): BigtableSparkConfBuilder = {
    bigtableMutateRowsAttemptTimeoutMs = Some(value)
    this
  }

  def setBigtableMutateRowsTotalTimeoutMs(value: String): BigtableSparkConfBuilder = {
    bigtableMutateRowsTotalTimeoutMs = Some(value)
    this
  }

  def setBigtableBatchMutateSize(value: Int): BigtableSparkConfBuilder = {
    bigtableBatchMutateSize = value
    this
  }

  def setBigtableEnableBatchMutateFlowControl(value: Boolean): BigtableSparkConfBuilder = {
    bigtableEnableBatchMutateFlowControl = value
    this
  }

  def build(): BigtableSparkConf = {
    new BigtableSparkConf(
      bigtableProjectId,
      bigtableInstanceId,
      bigtableAppProfileId,
      bigtableCreateNewTable,
      bigtableTimeRangeStart,
      bigtableTimeRangeEnd,
      bigtableWriteTimestamp,
      bigtableEmulatorPort,
      bigtablePushDownRowKeyFilters,
      bigtableReadRowsAttemptTimeoutMs,
      bigtableReadRowsTotalTimeoutMs,
      bigtableMutateRowsAttemptTimeoutMs,
      bigtableMutateRowsTotalTimeoutMs,
      bigtableBatchMutateSize,
      bigtableEnableBatchMutateFlowControl,
      maxReadRowsRetries
    )
  }
}

object BigtableSparkConfBuilder {
  def apply(): BigtableSparkConfBuilder = new BigtableSparkConfBuilder()
}

class BigtableSparkConf private[datasources] (
    val bigtableProjectId: Option[String],
    val bigtableInstanceId: Option[String],
    val bigtableAppProfileId: String,
    val bigtableCreateNewTable: Boolean,
    val bigtableTimeRangeStart: Option[Long],
    val bigtableTimeRangeEnd: Option[Long],
    val bigtableWriteTimestamp: Option[Long],
    val bigtableEmulatorPort: Option[Int],
    val bigtablePushDownRowKeyFilters: Boolean,
    val bigtableReadRowsAttemptTimeoutMs: Option[String],
    val bigtableReadRowsTotalTimeoutMs: Option[String],
    val bigtableMutateRowsAttemptTimeoutMs: Option[String],
    val bigtableMutateRowsTotalTimeoutMs: Option[String],
    val bigtableBatchMutateSize: Long,
    val bigtableEnableBatchMutateFlowControl: Boolean,
    val maxReadRowsRetries: Option[String]
) extends Serializable
