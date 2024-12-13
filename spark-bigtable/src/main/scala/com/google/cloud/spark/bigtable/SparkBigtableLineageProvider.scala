package com.google.cloud.spark.bigtable

import io.openlineage.spark.extension.OpenLineageExtensionProvider

class SparkBigtableLineageProvider extends OpenLineageExtensionProvider {

  def shadedPackage(): String = {
    "io.openlineage.spark.shade"
  }
}
