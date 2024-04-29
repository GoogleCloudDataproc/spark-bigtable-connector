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

/** This is the bigtable configuration. User can either set them in SparkConf, which
  * will take effect globally, or configure it per table, which will overwrite the value
  * set in SparkConf. If not set, the default value will take effect.
  * To access these variables inside Java, use the following import statement:
  * `import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.*;`
  * You can then use a syntax like `BIGTABLE_PROJECT_ID()` to access them.
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
