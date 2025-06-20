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

package com.google.cloud.spark.bigtable.datasources.config.application

object SparkWritesConfig {
  val WRITE_TIMESTAMP_CONFIG_KEY = "spark.bigtable.write.timestamp.milliseconds"
  val CREATE_NEW_TABLE_CONFIG_KEY = "spark.bigtable.create.new.table"

  private[datasources] def fromMap(conf: Map[String, String]): SparkWritesConfig = {
    SparkWritesConfig(
      conf.get(WRITE_TIMESTAMP_CONFIG_KEY).map(_.toLong),
      conf.get(CREATE_NEW_TABLE_CONFIG_KEY).map(_.toBoolean))
  }

  def apply(writeTimestamp: Option[Long],
            createNewTable: Option[Boolean]): SparkWritesConfig = {
    new SparkWritesConfig(
      writeTimestamp,
      createNewTable.getOrElse(false))
  }

  def apply(): SparkWritesConfig = SparkWritesConfig(None, None)
}

case class SparkWritesConfig private[datasources]
(writeTimestamp: Option[Long],
 createNewTable: Boolean) {
  def getValidationErrors: Set[String] = Set()
}