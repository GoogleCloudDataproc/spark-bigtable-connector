package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.cloud.bigtable.data.v2.BigtableDataSettings

trait ClientConfigTrait {
  def getValidationErrors: Set[String]
  def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit
  def debugString(): String
}
