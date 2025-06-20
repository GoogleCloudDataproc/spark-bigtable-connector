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

import com.google.api.gax.core.CredentialsProvider
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spark.bigtable.util.Reflector

import scala.util.{Failure, Success}

object CustomAuthConfig {
  val CUSTOM_CREDENTIALS_PROVIDER_CONFIG_KEY = "spark.bigtable.auth.credentials_provider"
  val CUSTOM_CREDENTIALS_PROVIDER_ARGS_CONFIG_KEY = "spark.bigtable.auth.credentials_provider.args"

  def fromMap(conf: Map[String, String]): CustomAuthConfig = {
    CustomAuthConfig(
      conf.get(CUSTOM_CREDENTIALS_PROVIDER_CONFIG_KEY),
      conf.filter {
        case (key, _) =>key.startsWith(
          CUSTOM_CREDENTIALS_PROVIDER_ARGS_CONFIG_KEY + ".")
      })
  }

  def apply(): CustomAuthConfig = CustomAuthConfig(None, Map())
}

case class CustomAuthConfig
(providerFQCN: Option[String],
 providerArgs: Map[String, String]) extends ClientConfigTrait {
  override def getValidationErrors: Set[String] = Set()

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    providerFQCN.foreach(className =>
      // First try constructor with a Map, then parameterless constructor
      Reflector
        .createVerifiedInstance[CredentialsProvider](className, providerArgs)
        .orElse(Reflector.createVerifiedInstance[CredentialsProvider](className)) match {
        case Success(provider) => settingsBuilder.setCredentialsProvider(provider)
        case Failure(e) => throw e
      })
  }

  def applyTableAdminSettings(settingsBuilder: BigtableTableAdminSettings.Builder): Unit = {
    providerFQCN.foreach(className =>
      // First try constructor with a Map, then parameterless constructor
      Reflector
        .createVerifiedInstance[CredentialsProvider](className, providerArgs)
        .orElse(Reflector.createVerifiedInstance[CredentialsProvider](className)) match {
        case Success(provider) => settingsBuilder.setCredentialsProvider(provider)
        case Failure(e) => throw e
      })
  }

  override def debugString(): String =
    s"""CustomAuthConfig(
       | providerFQCN: $providerFQCN,
       | providerArgs: ${providerArgs.map(kv => (kv._1, "*****"))}
       |)""".stripMargin
}