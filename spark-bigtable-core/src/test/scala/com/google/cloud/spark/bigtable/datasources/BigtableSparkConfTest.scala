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

    assert(conf.projectId.contains("expected-project"))
    assert(conf.instanceId.contains("expected-instance"))
    assert(conf.appProfileId.contains("expected-app-profile"))
    assert(conf.createNewTable)
    assert(conf.timeRangeStart.contains(12345))
    assert(conf.timeRangeEnd.contains(54321))
    assert(conf.writeTimestamp.contains(1111))
    assert(conf.emulatorPort.contains(1234))
    assert(!conf.pushDownRowKeyFilters)
    assert(conf.readRowsAttemptTimeoutMs.contains("9876"))
    assert(conf.readRowsTotalTimeoutMs.contains("1000"))
    assert(conf.mutateRowsAttemptTimeoutMs.contains("9999"))
    assert(conf.mutateRowsTotalTimeoutMs.contains("2222"))
    assert(conf.batchMutateSize == 2211)
    assert(conf.enableBatchMutateFlowControl)
    assert(conf.customCredentialsProviderFQCN.contains("some-class"))
    assert(conf.customCredentialsProviderArgs.size == 1)
    assert(conf.customCredentialsProviderArgs.get("spark.bigtable.auth.credentials_provider.args.param1").contains("some-param"))
  }

  test("Validate default values") {
    // For now validation of parameters is done on usage. We validate default
    // values since changing them can be considered a breaking change.

    val conf = BigtableSparkConfBuilder().fromMap(Map()).build()

    assert(conf.projectId.isEmpty)
    assert(conf.instanceId.isEmpty)
    assert(conf.appProfileId.contains("default"))
    assert(!conf.createNewTable)
    assert(conf.timeRangeStart.isEmpty)
    assert(conf.timeRangeEnd.isEmpty)
    assert(conf.writeTimestamp.isEmpty)
    assert(conf.emulatorPort.isEmpty)
    assert(conf.pushDownRowKeyFilters)
    assert(conf.readRowsAttemptTimeoutMs.isEmpty)
    assert(conf.readRowsTotalTimeoutMs.isEmpty)
    assert(conf.mutateRowsAttemptTimeoutMs.isEmpty)
    assert(conf.mutateRowsTotalTimeoutMs.isEmpty)
    assert(conf.batchMutateSize == 100)
    assert(!conf.enableBatchMutateFlowControl)
    assert(conf.customCredentialsProviderFQCN.isEmpty)
    assert(conf.customCredentialsProviderArgs.isEmpty)
  }

  test("Validate custom auth provider arguments are handled") {
    val map = Map(
      ("spark.bigtable.auth.credentials_provider.args.param1", "some-param"),
      ("spark.bigtable.auth.credentials_provider.args.param2", "another-param"),
    )

    val conf = BigtableSparkConfBuilder().fromMap(map).build()

    assert(conf.customCredentialsProviderArgs.size == 2)
    assert(conf.customCredentialsProviderArgs.get("spark.bigtable.auth.credentials_provider.args.param1").contains("some-param"))
    assert(conf.customCredentialsProviderArgs.get("spark.bigtable.auth.credentials_provider.args.param2").contains("another-param"))
  }
}
