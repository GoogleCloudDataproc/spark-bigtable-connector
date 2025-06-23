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

import com.google.api.gax.batching.{BatchingSettings, FlowControlSettings}
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spark.bigtable.datasources.config.client.BatchMutateConfig.{BATCH_MUTATE_SIZE_CONFIG_KEY, MAX_BATCH_MUTATE_SIZE}

import scala.math

object BatchMutateConfig {
  val BATCH_MUTATE_SIZE_CONFIG_KEY = "spark.bigtable.batch.mutate.size"

  val MAX_BATCH_MUTATE_SIZE = 100000

  val ENABLE_BATCH_MUTATE_FLOW_CONTROL_CONFIG_KEY =
    "spark.bigtable.enable.batch_mutate.flow_control"

  private [datasources] def fromMap(conf: Map[String, String]): BatchMutateConfig = {
    BatchMutateConfig(
      conf.get(ENABLE_BATCH_MUTATE_FLOW_CONTROL_CONFIG_KEY).map(_.toBoolean),
      conf.get(BATCH_MUTATE_SIZE_CONFIG_KEY).map(_.toLong))
  }

  def apply(): BatchMutateConfig = BatchMutateConfig(None, None)
}

case class BatchMutateConfig
(enableBatchMutateFlowControl: Option[Boolean],
 batchMutateSize: Option[Long]) extends ClientConfigTrait {
  override def getValidationErrors: Set[String] = {
    batchMutateSize match {
      case Some(size)
        if size < 1 || size > MAX_BATCH_MUTATE_SIZE =>
        Set(f"$BATCH_MUTATE_SIZE_CONFIG_KEY must be between 1 and $MAX_BATCH_MUTATE_SIZE")
      case _ => Set()
    }
  }

  override def debugString(): String =
    s"""BatchMutateConfig(
       | enableBatchMutateFlowControl = $enableBatchMutateFlowControl
       | batchMutateSize = $batchMutateSize
       |)""".stripMargin

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    enableBatchMutateFlowControl.foreach(enable => settingsBuilder.setBulkMutationFlowControl(enable))

    batchMutateSize.foreach(size => {
      settingsBuilder
        .stubSettings
        .bulkMutateRowsSettings
        .setBatchingSettings(
          getNewBatchingSettings(
            settingsBuilder
              .stubSettings()
              .bulkMutateRowsSettings()
              .getBatchingSettings, size)
        )
    })
  }

  private def getNewBatchingSettings(currentSettings: BatchingSettings, size: Long): BatchingSettings = {
    currentSettings
      .toBuilder
      .setElementCountThreshold(size)
      .setFlowControlSettings(
        getNewFlowControlSettings(currentSettings.getFlowControlSettings, size)
      )
      .build
  }

  private def getNewFlowControlSettings(currentSettings: FlowControlSettings, size: Long): FlowControlSettings = {
    // If the batch size is higher than the maximum number of outstanding
    // elements we need to increase the max
    val newValue = math.max(currentSettings.getMaxOutstandingElementCount, size + 1)

    currentSettings
      .toBuilder
      .setMaxOutstandingElementCount(newValue)
      .build
  }
}