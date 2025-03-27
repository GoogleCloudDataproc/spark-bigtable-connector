package com.google.cloud.spark.bigtable

import io.openlineage.spark.extension.OpenLineageExtensionProvider
import io.openlineage.spark.shade.extension.v1.lifecycle.plan.SparkOpenLineageExtensionVisitor

class SparkBigtableLineageProvider extends OpenLineageExtensionProvider {

  override def getVisitorClassName: String = {
     classOf[SparkOpenLineageExtensionVisitor].getCanonicalName
  }

  def shadedPackage(): String =
    "com.google.cloud.spark.bigtable.repackaged.io.openlineage.spark.shade"
}
