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

object SparkScanConfig {
  val TIME_RANGE_START_CONFIG_KEY =
    "spark.bigtable.read.timerange.start.milliseconds"

  val TIME_RANGE_END_CONFIG_KEY = "spark.bigtable.read.timerange.end.milliseconds"

  val PUSH_DOWN_FILTERS_CONFIG_KEY =
    "spark.bigtable.push.down.row.key.filters"

  private[datasources] def fromMap(conf: Map[String, String]): SparkScanConfig = {
    SparkScanConfig(
      conf.get(TIME_RANGE_START_CONFIG_KEY).map(_.toLong),
      conf.get(TIME_RANGE_END_CONFIG_KEY).map(_.toLong),
      conf.get(PUSH_DOWN_FILTERS_CONFIG_KEY).map(_.toBoolean))
  }

  def apply(timeRangeStart: Option[Long],
            timeRangeEnd: Option[Long],
            pushDownRowKeyFilters: Option[Boolean]): SparkScanConfig = {
    new SparkScanConfig(
      timeRangeStart,
      timeRangeEnd,
      pushDownRowKeyFilters.getOrElse(true))
  }

  def apply(): SparkScanConfig = SparkScanConfig(None, None, None)
}

case class SparkScanConfig private[datasources]
(timeRangeStart: Option[Long],
 timeRangeEnd: Option[Long],
 pushDownRowKeyFilters: Boolean) {
  def getValidationErrors: Set[String] = Set()
}