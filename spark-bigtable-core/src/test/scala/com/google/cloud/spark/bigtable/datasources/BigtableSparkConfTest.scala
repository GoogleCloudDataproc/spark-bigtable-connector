package com.google.cloud.spark.bigtable.datasources

import com.google.cloud.spark.bigtable.Logging
import org.scalatest.funsuite.AnyFunSuite

class BigtableSparkConfTest extends AnyFunSuite
  with Logging {

  test("Should set all known parameters") {
    val paramsMap = Map(
      ("spark.bigtable.project.id", "expected-project"),
      ("spark.bigtable.instance.id", "expected-instance"),
      ("spark.bigtable.app_profile.id", "expected-app-profile"),
      ("spark.bigtable.create.new.table", "true"),
      ("spark.bigtable.read.timerange.start.milliseconds", "12345"),
      ("spark.bigtable.read.timerange.end.milliseconds", "54321"),
      ("spark.bigtable.write.timestamp.milliseconds", "1111"),
      ("spark.bigtable.emulator.port", "1234"),
      ("spark.bigtable.push.down.row.key.filters", "false"),
      ("spark.bigtable.read.rows.attempt.timeout.milliseconds", "9876"),
      ("spark.bigtable.read.rows.total.timeout.milliseconds", "1000"),
      ("spark.bigtable.mutate.rows.attempt.timeout.milliseconds", "9999"),
      ("spark.bigtable.mutate.rows.total.timeout.milliseconds", "2222"),
      ("spark.bigtable.batch.mutate.size", "2211"),
      ("spark.bigtable.enable.batch_mutate.flow_control", "true"),
      ("spark.bigtable.auth.credentials_provider", "some-class"),
      ("spark.bigtable.auth.credentials_provider.args.param1", "some-param"),
    )

    val conf = BigtableSparkConfBuilder().fromMap(paramsMap).build()

    assert(conf.bigtableClientConfig.bigtableResourcesConfig.projectId.contains("expected-project"))
    assert(conf.bigtableClientConfig.bigtableResourcesConfig.instanceId.contains("expected-instance"))
    assert(conf.bigtableClientConfig.bigtableResourcesConfig.appProfileId.contains("expected-app-profile"))
    assert(conf.appConfig.sparkWritesConfig.createNewTable)
    assert(conf.appConfig.sparkScanConfig.timeRangeStart.contains(12345))
    assert(conf.appConfig.sparkScanConfig.timeRangeEnd.contains(54321))
    assert(conf.appConfig.sparkWritesConfig.writeTimestamp.contains(1111))
    assert(conf.bigtableClientConfig.emulatorPort.contains(1234))
    assert(!conf.appConfig.sparkScanConfig.pushDownRowKeyFilters)
    assert(conf.bigtableClientConfig.readRowsConfig.attemptTimeout.contains(9876))
    assert(conf.bigtableClientConfig.readRowsConfig.totalTimeout.contains(1000))
    assert(conf.bigtableClientConfig.mutateRowsConfig.attemptTimeout.contains(9999))
    assert(conf.bigtableClientConfig.mutateRowsConfig.totalTimeout.contains(2222))
    assert(conf.bigtableClientConfig.batchMutateConfig.batchMutateSize.contains(2211))
    assert(conf.bigtableClientConfig.batchMutateConfig.enableBatchMutateFlowControl.contains(true))
    assert(conf.bigtableClientConfig.customAuthConfig.providerFQCN.contains("some-class"))
    assert(conf.bigtableClientConfig.customAuthConfig.providerArgs.size == 1)
    assert(conf.bigtableClientConfig.customAuthConfig.providerArgs.get("spark.bigtable.auth.credentials_provider.args.param1").contains("some-param"))
  }

  test("Validate default values") {
    // For now validation of parameters is done on usage. We validate default
    // values since changing them can be considered a breaking change.

    val conf = BigtableSparkConfBuilder()
      .fromMap(Map())
      .setProjectId("some-project")
      .setInstanceId("some-instance").build()

    assert(conf.bigtableClientConfig.bigtableResourcesConfig.projectId.contains(""))
    assert(conf.bigtableClientConfig.bigtableResourcesConfig.instanceId.contains(""))
    assert(conf.bigtableClientConfig.bigtableResourcesConfig.appProfileId.isEmpty)
    assert(!conf.appConfig.sparkWritesConfig.createNewTable)
    assert(conf.appConfig.sparkScanConfig.timeRangeStart.isEmpty)
    assert(conf.appConfig.sparkScanConfig.timeRangeEnd.isEmpty)
    assert(conf.appConfig.sparkWritesConfig.writeTimestamp.isEmpty)
    assert(conf.bigtableClientConfig.emulatorPort.isEmpty)
    assert(conf.appConfig.sparkScanConfig.pushDownRowKeyFilters)
    assert(conf.bigtableClientConfig.readRowsConfig.attemptTimeout.isEmpty)
    assert(conf.bigtableClientConfig.readRowsConfig.totalTimeout.isEmpty)
    assert(conf.bigtableClientConfig.mutateRowsConfig.attemptTimeout.isEmpty)
    assert(conf.bigtableClientConfig.mutateRowsConfig.totalTimeout.isEmpty)
    assert(conf.bigtableClientConfig.batchMutateConfig.batchMutateSize.isEmpty)
    assert(conf.bigtableClientConfig.batchMutateConfig.enableBatchMutateFlowControl.isEmpty)
    assert(conf.bigtableClientConfig.customAuthConfig.providerFQCN.isEmpty)
    assert(conf.bigtableClientConfig.customAuthConfig.providerArgs.isEmpty)
  }

  test("Validate custom auth provider arguments are handled") {
    val map = Map(
      ("spark.bigtable.auth.credentials_provider.args.param1", "some-param"),
      ("spark.bigtable.auth.credentials_provider.args.param2", "another-param"),
    )

    val conf = BigtableSparkConfBuilder()
      .fromMap(map)
      .setProjectId("some-project")
      .setInstanceId("some-instance")
      .build()

    assert(conf.bigtableClientConfig.customAuthConfig.providerArgs.size == 2)
    assert(conf.bigtableClientConfig.customAuthConfig.providerArgs.get("spark.bigtable.auth.credentials_provider.args.param1").contains("some-param"))
    assert(conf.bigtableClientConfig.customAuthConfig.providerArgs.get("spark.bigtable.auth.credentials_provider.args.param2").contains("another-param"))
  }
}
