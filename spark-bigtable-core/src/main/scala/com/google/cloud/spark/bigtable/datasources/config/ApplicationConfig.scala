package com.google.cloud.spark.bigtable.datasources.config

import com.google.cloud.spark.bigtable.datasources.config.application.{SparkScanConfig, SparkWritesConfig}

// Groups configurations related to the spark application itself
case class ApplicationConfig(sparkScanConfig: SparkScanConfig,
                             sparkWritesConfig: SparkWritesConfig) {
  def getValidationErrors(): Set[String] = Set()
}
