package com.google.cloud.spark.bigtable

import com.google.cloud.spark.bigtable.datasources.{BigtableSparkConf, BigtableSparkConfBuilder}
import org.scalatest.funsuite.AnyFunSuite

class MaxVersionsFeatureTest extends AnyFunSuite {

  test("BigtableSparkConfBuilder should set maxVersions correctly") {
    val conf = new BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .setMaxVersions(5)
      .build()

    assert(conf.appConfig.sparkScanConfig.maxVersions === Some(5))
  }

  test("BigtableSparkConfBuilder should handle None maxVersions") {
    val conf = new BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .build()

    assert(conf.appConfig.sparkScanConfig.maxVersions === None)
  }

  test("BigtableSparkConfBuilder should validate positive maxVersions") {
    val confBuilder = new BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .setMaxVersions(0)

    val exception = intercept[IllegalArgumentException] {
      confBuilder.build()
    }

    assert(exception.getMessage.contains("must be a positive integer"))
  }

  test("BigtableSparkConfBuilder should validate negative maxVersions") {
    val confBuilder = new BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .setMaxVersions(-1)

    val exception = intercept[IllegalArgumentException] {
      confBuilder.build()
    }

    assert(exception.getMessage.contains("must be a positive integer"))
  }

  test("BIGTABLE_MAX_VERSIONS configuration key should be correct") {
    assert(BigtableSparkConf.BIGTABLE_MAX_VERSIONS === "spark.bigtable.read.max.versions")
  }

  test("maxVersions should be parsed from configuration map") {
    val configMap = Map(
      "spark.bigtable.project.id" -> "test-project",
      "spark.bigtable.instance.id" -> "test-instance",
      "spark.bigtable.read.max.versions" -> "3"
    )

    val confBuilder = new BigtableSparkConfBuilder()
    val conf = confBuilder.fromMap(configMap).build()

    assert(conf.appConfig.sparkScanConfig.maxVersions === Some(3))
  }

  test("BigtableRDD.readRDD should propagate maxVersions to BigtableTableScanRDD") {
    val conf = new BigtableSparkConfBuilder()
      .setProjectId("p")
      .setInstanceId("i")
      .setMaxVersions(4)
      .build()
    val sc = new org.apache.spark.SparkContext("local[1]", "test")
    try {
      val rddApi = new BigtableRDD(sc)
      val underlying = rddApi.readRDD("tbl", conf)
      // Reflection to access private field 'maxVersions' in BigtableTableScanRDD; if structure changes update this test.
      val clazz = underlying.getClass
      val field = clazz.getDeclaredField("maxVersions")
      field.setAccessible(true)
      val value = field.get(underlying).asInstanceOf[Option[Int]]
      assert(value.contains(4))
    } finally {
      sc.stop()
    }
  }
}
