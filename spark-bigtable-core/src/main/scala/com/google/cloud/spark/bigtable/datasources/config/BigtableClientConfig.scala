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

package com.google.cloud.spark.bigtable.datasources.config

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spark.bigtable.datasources.config.client.{BatchMutateConfig, BigtableResourcesConfig, CustomAuthConfig, MutateRowsRpcConfig, ReadRowsRpcConfig, UserAgentConfig}

object BigtableClientConfig {
  val BIGTABLE_EMULATOR_PORT_CONFIG_KEY = "spark.bigtable.emulator.port"
}

// Groups all settings that are applied to the bigtable client at runtime.
// Any bigtable client setting must be included here so
// BigtableDataClientBuilder can properly reuse clients where appropriate
case class BigtableClientConfig(bigtableResourcesConfig: BigtableResourcesConfig,
                                emulatorPort: Option[Int],
                                readRowsConfig: ReadRowsRpcConfig,
                                mutateRowsConfig: MutateRowsRpcConfig,
                                batchMutateConfig: BatchMutateConfig,
                                customAuthConfig: CustomAuthConfig,
                                userAgentConfig: UserAgentConfig) {
  def getValidationErrors(): Set[String] = {
    bigtableResourcesConfig.getValidationErrors ++
      readRowsConfig.getValidationErrors ++
      mutateRowsConfig.getValidationErrors ++
      batchMutateConfig.getValidationErrors ++
      customAuthConfig.getValidationErrors ++
      userAgentConfig.getValidationErrors
  }

  def getDataSettings(): BigtableDataSettings = {
    val settingsBuilder = emulatorPort match {
      case Some(port) => BigtableDataSettings.newBuilderForEmulator(port)
      case _ => BigtableDataSettings.newBuilder()
    }

    bigtableResourcesConfig.applySettings(settingsBuilder)
    readRowsConfig.applySettings(settingsBuilder)
    mutateRowsConfig.applySettings(settingsBuilder)
    batchMutateConfig.applySettings(settingsBuilder)
    customAuthConfig.applySettings(settingsBuilder)
    userAgentConfig.applySettings(settingsBuilder)

    settingsBuilder.build()
  }

  def getTableAdminSettings(): BigtableTableAdminSettings = {
    val settingsBuilder = {
      emulatorPort match {
        case Some(port) =>
          BigtableTableAdminSettings.newBuilderForEmulator(port)
        case None => BigtableTableAdminSettings.newBuilder()
      }
    }

    bigtableResourcesConfig.applyTableAdminSettings(settingsBuilder)
    userAgentConfig.applyTableAdminSettings(settingsBuilder)
    customAuthConfig.applyTableAdminSettings(settingsBuilder)
    settingsBuilder.build()
  }

  override def toString: String = {
    s"""BigtableClientConfig(
       | bigtableResourcesConfig = $bigtableResourcesConfig
       | emulatorPort = $emulatorPort
       | readRowsConfig = $readRowsConfig
       | mutateRowsConfig = $mutateRowsConfig
       | batchMutateConfig = $batchMutateConfig
       | customAuthConfig = $customAuthConfig
       |)""".stripMargin
  }
}
