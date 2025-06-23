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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spark.bigtable.datasources.config.client.BigtableResourcesConfig.{INSTANCE_ID_CONFIG_KEY, PROJECT_ID_CONFIG_KEY}

object BigtableResourcesConfig {
  val PROJECT_ID_CONFIG_KEY = "spark.bigtable.project.id"
  val INSTANCE_ID_CONFIG_KEY = "spark.bigtable.instance.id"
  val APP_PROFILE_ID_CONFIG_KEY = "spark.bigtable.app_profile.id"

  def fromMap(conf: Map[String, String]): BigtableResourcesConfig = {
    BigtableResourcesConfig(
      conf.get(PROJECT_ID_CONFIG_KEY),
      conf.get(INSTANCE_ID_CONFIG_KEY),
      conf.get(APP_PROFILE_ID_CONFIG_KEY))
  }

  def apply(projectId: Option[String],
            instanceId: Option[String],
            appProfileId: Option[String]): BigtableResourcesConfig = {
    new BigtableResourcesConfig(
      projectId.getOrElse(""),
      instanceId.getOrElse(""),
      appProfileId)
  }

  def apply(): BigtableResourcesConfig = BigtableResourcesConfig(None, None, None)
}

case class BigtableResourcesConfig
(projectId: String,
 instanceId: String,
 appProfileId: Option[String]) extends ClientConfigTrait {
  override def getValidationErrors: Set[String] = {
    var errors = Set[String]()

    if (projectId.isEmpty) {
      errors += f"$PROJECT_ID_CONFIG_KEY must be set"
    }

    if (instanceId.isEmpty) {
      errors += f"$INSTANCE_ID_CONFIG_KEY must be set"
    }

    errors
  }

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    appProfileId.foreach(appProfile => settingsBuilder.setAppProfileId(appProfile))
    settingsBuilder
      .setProjectId(projectId)
      .setInstanceId(instanceId)
  }

  def applyTableAdminSettings(settingsBuilder: BigtableTableAdminSettings.Builder): Unit = {
    settingsBuilder
      .setProjectId(projectId)
      .setInstanceId(instanceId)
  }

  override def debugString(): String =
    s"""BigtableResourcesConfig(
       | projectId: $projectId,
       | instanceId: $instanceId,
       | appProfileId: $appProfileId
       |)""".stripMargin
}
