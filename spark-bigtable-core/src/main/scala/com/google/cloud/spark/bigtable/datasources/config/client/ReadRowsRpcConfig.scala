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

object ReadRowsRpcConfig {
  val ATTEMPT_TIMEOUT_MS_CONFIG_KEY =
    "spark.bigtable.read.rows.attempt.timeout.milliseconds"

  val TOTAL_TIMEOUT_MS_CONFIG_KEY =
    "spark.bigtable.read.rows.total.timeout.milliseconds"

  val RETRIES_CONFIG_KEY =
    "spark.bigtable.max.read.rows.retries"

  def fromMap(conf: Map[String, String]): ReadRowsRpcConfig = {
    ReadRowsRpcConfig(
      conf.get(ATTEMPT_TIMEOUT_MS_CONFIG_KEY).map(_.toLong),
      conf.get(TOTAL_TIMEOUT_MS_CONFIG_KEY).map(_.toLong),
      conf.get(RETRIES_CONFIG_KEY).map(_.toInt))
  }

  def apply(): ReadRowsRpcConfig = ReadRowsRpcConfig(None, None, None)
}

case class ReadRowsRpcConfig
(attemptTimeout: Option[Long],
 totalTimeout: Option[Long],
 maxRetries: Option[Int]) extends ClientConfigTrait {
  override def getValidationErrors: Set[String] = Set()

  override def debugString(): String =
    s"""ReadRowsRpcConfig(
       | attemptTimeout = $attemptTimeout
       | totalTimeout = $totalTimeout
       | maxRetries = $maxRetries
       |)""".stripMargin

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    attemptTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setInitialRpcTimeout(Duration.ofMillis(timeout))
    )
    totalTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setTotalTimeout(Duration.ofMillis(timeout))
    )
    maxRetries.foreach(retries =>
      settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setMaxAttempts(retries)
    )
  }
}