package com.google.cloud.spark.bigtable

import io.openlineage.spark.extension.OpenLineageExtensionProvider

class SparkBigtableLineageProvider extends OpenLineageExtensionProvider {

  def shadedPackage(): String =
    "com.google.cloud.spark.bigtable.repackaged.io.openlineage.spark.shade"
}
