/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import org.threeten.bp.Duration

object MutateRowsRpcConfig {
  val ATTEMPT_TIMEOUT_MS_CONFIG_KEY =
    "spark.bigtable.mutate.rows.attempt.timeout.milliseconds"

  val TOTAL_TIMEOUT_MS_CONFIG_KEY =
    "spark.bigtable.mutate.rows.total.timeout.milliseconds"

  def fromMap(conf: Map[String, String]): MutateRowsRpcConfig = {
    MutateRowsRpcConfig(
      conf.get(ATTEMPT_TIMEOUT_MS_CONFIG_KEY).map(_.toLong),
      conf.get(TOTAL_TIMEOUT_MS_CONFIG_KEY).map(_.toLong))
  }

  def apply(): MutateRowsRpcConfig = MutateRowsRpcConfig(None, None)
}

case class MutateRowsRpcConfig
(attemptTimeout: Option[Long],
 totalTimeout: Option[Long]) extends ClientConfigTrait {
  override def getValidationErrors: Set[String] = Set()

  override def debugString(): String =
    s"""MutateRowsRpcConfig(
       | attemptTimeout = $attemptTimeout
       | totalTimeout = $totalTimeout
       |)""".stripMargin

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    attemptTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .bulkMutateRowsSettings()
        .retrySettings()
        .setInitialRpcTimeout(Duration.ofMillis(timeout))
    )
    totalTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .bulkMutateRowsSettings()
        .retrySettings()
        .setTotalTimeout(Duration.ofMillis(timeout))
    )
  }
}