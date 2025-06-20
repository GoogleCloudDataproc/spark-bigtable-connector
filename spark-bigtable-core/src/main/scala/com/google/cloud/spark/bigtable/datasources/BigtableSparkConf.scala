/*
 * Copyright 2024 Google LLC
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

package com.google.cloud.spark.bigtable.datasources

import com.google.api.core.ObsoleteApi
import com.google.cloud.spark.bigtable.datasources.config.application.{SparkScanConfig, SparkWritesConfig}
import com.google.cloud.spark.bigtable.datasources.config.client.{BatchMutateConfig, BigtableResourcesConfig, CustomAuthConfig, MutateRowsRpcConfig, ReadRowsRpcConfig, UserAgentConfig}
import com.google.cloud.spark.bigtable.datasources.config.{ApplicationConfig, BigtableClientConfig}
import org.apache.yetus.audience.InterfaceAudience

/** This is the bigtable configuration. Users can either set them in SparkConf, which
  * will take effect globally, or configure it per table, which will overwrite the value
  * set in SparkConf. If not set, the default value will take effect.
  * To access these variables inside Java, use the following import statement:
  * `import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.*;`
  * You can then use a syntax like `BIGTABLE_PROJECT_ID()` to access them.
  * When using RDDs, you can use a config builder object and config setter methods,
  * e.g., `new BigtableSparkConfBuilder().setProjectId(someProjectId).build()`
  * to create the `BigtableSparkConf` object with specific configs.
  */
@InterfaceAudience.Public
object BigtableSparkConf {
  /** The Bigtable project id. */
  val BIGTABLE_PROJECT_ID: String = BigtableResourcesConfig.PROJECT_ID_CONFIG_KEY

  /** The Bigtable instance id. */
  val BIGTABLE_INSTANCE_ID: String = BigtableResourcesConfig.INSTANCE_ID_CONFIG_KEY

  /** The Bigtable app profile. */
  val BIGTABLE_APP_PROFILE_ID: String = BigtableResourcesConfig.APP_PROFILE_ID_CONFIG_KEY

  @ObsoleteApi("This field is obsolete is will be removed on a future version. To use a default configuration simply leave it unset")
  val DEFAULT_BIGTABLE_APP_PROFILE_ID = "default"

  /** The timestamp to set on all cells when writing. If not specified,
    * all cells will have the current timestamp.
    */
  val BIGTABLE_WRITE_TIMESTAMP: String = SparkWritesConfig.WRITE_TIMESTAMP_CONFIG_KEY

  /** Specifying whether to create a new Bigtable table or not. */
  val BIGTABLE_CREATE_NEW_TABLE: String = SparkWritesConfig.CREATE_NEW_TABLE_CONFIG_KEY
  @ObsoleteApi("This field is obsolete is will be removed on a future version. To use a default configuration simply leave it unset")
  val DEFAULT_BIGTABLE_CREATE_NEW_TABLE = false

  /** The timestamp start range to filter cell values. */
  val BIGTABLE_TIMERANGE_START: String = SparkScanConfig.TIME_RANGE_START_CONFIG_KEY

  /** The timestamp end range to filter cell values. */
  val BIGTABLE_TIMERANGE_END: String = SparkScanConfig.TIME_RANGE_END_CONFIG_KEY

  /** Specifying whether to push row key filters to Bigtable or not. */
  val BIGTABLE_PUSH_DOWN_ROW_KEY_FILTERS: String = SparkScanConfig.PUSH_DOWN_FILTERS_CONFIG_KEY

  /** By default, push down all filters (row key, column value, etc.) down. */
  @ObsoleteApi("This field is obsolete is will be removed on a future version. To use a default configuration simply leave it unset")
  val DEFAULT_BIGTABLE_PUSH_DOWN_FILTERS = true

  /** The timeout duration for read rows request attempts. */
  val BIGTABLE_READ_ROWS_ATTEMPT_TIMEOUT_MS: String = ReadRowsRpcConfig.ATTEMPT_TIMEOUT_MS_CONFIG_KEY

  /** The total timeout duration for read rows request. */
  val BIGTABLE_READ_ROWS_TOTAL_TIMEOUT_MS: String = ReadRowsRpcConfig.TOTAL_TIMEOUT_MS_CONFIG_KEY

  /** Used for internal testing and not officially supported. */
  private[datasources] val MAX_READ_ROWS_RETRIES = ReadRowsRpcConfig.RETRIES_CONFIG_KEY

  /** The timeout duration for bulk mutation request attempts. */
  val BIGTABLE_MUTATE_ROWS_ATTEMPT_TIMEOUT_MS: String = MutateRowsRpcConfig.ATTEMPT_TIMEOUT_MS_CONFIG_KEY

  /** The total timeout duration for bulk mutation requests. */
  val BIGTABLE_MUTATE_ROWS_TOTAL_TIMEOUT_MS: String = MutateRowsRpcConfig.TOTAL_TIMEOUT_MS_CONFIG_KEY

  /** The emulator port. */
  val BIGTABLE_EMULATOR_PORT = BigtableClientConfig.BIGTABLE_EMULATOR_PORT_CONFIG_KEY

  /** The batch size for batch mutation requests. Maximum is 100K. */
  val BIGTABLE_BATCH_MUTATE_SIZE: String = BatchMutateConfig.BATCH_MUTATE_SIZE_CONFIG_KEY

  @ObsoleteApi("Batch size default is obsolete and will be removed in the future. To use a default batch size simply leave this option as None")
  val BIGTABLE_DEFAULT_BATCH_MUTATE_SIZE = 100

  val BIGTABLE_MAX_BATCH_MUTATE_SIZE = 100000

  /** Signals if batch mutations should use flow control. */
  val BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL = BatchMutateConfig.ENABLE_BATCH_MUTATE_FLOW_CONTROL_CONFIG_KEY

  @ObsoleteApi("Flow control default enabled is obsolete and will be removed in the future. Leave this option set to None if you want the default behavior")
  val DEFAULT_BIGTABLE_ENABLE_BATCH_MUTATE_FLOW_CONTROL = false

  /** Fully qualified class name of a class implementing com.google.api.gax.core.CredentialsProvider */
  val BIGTABLE_CUSTOM_CREDENTIALS_PROVIDER: String = CustomAuthConfig.CUSTOM_CREDENTIALS_PROVIDER_CONFIG_KEY

  /** Optional arguments to be passed to a custom CredentialsProvider. Multiple arguments may be passed by setting
    * multiple values using this key prefix and a `.` */
  val BIGTABLE_CUSTOM_CREDENTIALS_PROVIDER_ARGS: String = CustomAuthConfig.CUSTOM_CREDENTIALS_PROVIDER_ARGS_CONFIG_KEY
}

class BigtableSparkConfBuilder extends Serializable {
  private var resourcesConf = BigtableResourcesConfig()
  private var emulatorPort: Option[Int] = None
  private var writeConf = SparkWritesConfig()
  private var scanConf = SparkScanConfig()
  private var readRowsConf = ReadRowsRpcConfig()
  private var mutateRowsConf = MutateRowsRpcConfig()
  private var batchMutateConf = BatchMutateConfig()
  private var customAuthConf = CustomAuthConfig()
  private var userAgentConf = UserAgentConfig()

  private def this(bigtableClientConfig: BigtableClientConfig,
                   applicationConfig: ApplicationConfig) = {
    this()
    resourcesConf = bigtableClientConfig.bigtableResourcesConfig
    emulatorPort = bigtableClientConfig.emulatorPort
    writeConf = applicationConfig.sparkWritesConfig
    scanConf = applicationConfig.sparkScanConfig
    readRowsConf = bigtableClientConfig.readRowsConfig
    mutateRowsConf = bigtableClientConfig.mutateRowsConfig
    batchMutateConf = bigtableClientConfig.batchMutateConfig
    customAuthConf = bigtableClientConfig.customAuthConfig
    userAgentConf = bigtableClientConfig.userAgentConfig
  }

  // This function is package-private to allow internal setup when using DataFrames
  // (since Spark SQL passes a Map<String, String> object). However, for external use with RDDs,
  // users need to use the setter methods to set the configs.
  private[bigtable] def fromMap(conf: Map[String, String]): BigtableSparkConfBuilder = {
    resourcesConf = BigtableResourcesConfig.fromMap(conf)
    writeConf = SparkWritesConfig.fromMap(conf)
    scanConf = SparkScanConfig.fromMap(conf)
    readRowsConf = ReadRowsRpcConfig.fromMap(conf)
    mutateRowsConf = MutateRowsRpcConfig.fromMap(conf)
    batchMutateConf = BatchMutateConfig.fromMap(conf)
    customAuthConf = CustomAuthConfig.fromMap(conf)

    emulatorPort = conf.get(BigtableSparkConf.BIGTABLE_EMULATOR_PORT).map(_.toInt)

    this
  }

  def setProjectId(value: String): BigtableSparkConfBuilder = {
    resourcesConf = resourcesConf.copy(projectId = value)
    this
  }

  def setInstanceId(value: String): BigtableSparkConfBuilder = {
    resourcesConf = resourcesConf.copy(instanceId = value)
    this
  }

  def setAppProfileId(value: String): BigtableSparkConfBuilder = {
    resourcesConf = resourcesConf.copy(appProfileId = Some(value))
    this
  }

  @ObsoleteApi("This overload is deprecated and will be removed on a future version. Use setReadRowsAttemptTimeoutMs(Long) instead.")
  def setReadRowsAttemptTimeoutMs(value: String): BigtableSparkConfBuilder = {
    setReadRowsAttemptTimeoutMs(value.toLong)
  }

  def setReadRowsAttemptTimeoutMs(value: Long): BigtableSparkConfBuilder = {
    readRowsConf = readRowsConf.copy(attemptTimeout = Some(value))
    this
  }

  @ObsoleteApi("This overload is deprecated and will be removed on a future version. Use setReadRowsTotalTimeoutMs(Long) instead.")
  def setReadRowsTotalTimeoutMs(value: String): BigtableSparkConfBuilder = {
    setReadRowsTotalTimeoutMs(value.toLong)
  }

  def setReadRowsTotalTimeoutMs(value: Long): BigtableSparkConfBuilder = {
    readRowsConf = readRowsConf.copy(totalTimeout = Some(value))
    this
  }

  @ObsoleteApi("This overload is deprecated and will be removed on a future version. Use setMutateRowsAttemptTimeoutMs(Long) instead.")
  def setMutateRowsAttemptTimeoutMs(value: String): BigtableSparkConfBuilder = {
    setMutateRowsAttemptTimeoutMs(value.toLong)
  }

  def setMutateRowsAttemptTimeoutMs(value: Long): BigtableSparkConfBuilder = {
    mutateRowsConf = mutateRowsConf.copy(attemptTimeout = Some(value))
    this
  }

  @ObsoleteApi("This overload is deprecated and will be removed on a future version. Use setMutateRowsTotalTimeoutMs(Long) instead.")
  def setMutateRowsTotalTimeoutMs(value: String): BigtableSparkConfBuilder = {
    setMutateRowsTotalTimeoutMs(value.toLong)
  }

  def setMutateRowsTotalTimeoutMs(value: Long): BigtableSparkConfBuilder = {
    mutateRowsConf = mutateRowsConf.copy(totalTimeout = Some(value))
    this
  }

  def setBatchMutateSize(value: Int): BigtableSparkConfBuilder = {
    batchMutateConf = batchMutateConf.copy(batchMutateSize = Some(value))
    this
  }

  def setEnableBatchMutateFlowControl(value: Boolean): BigtableSparkConfBuilder = {
    batchMutateConf = batchMutateConf.copy(enableBatchMutateFlowControl = Some(value))
    this
  }

  def setSparkVersion(value: String): BigtableSparkConfBuilder = {
    userAgentConf = userAgentConf.copy(sparkVersion = value)
    this
  }

  def setUserAgentSourceInfo(value: String): BigtableSparkConfBuilder = {
    userAgentConf = userAgentConf.copy(sourceInfo = value)
    this
  }

  def setCustomAuthenticationProvider(className: String,
                                      params: Map[String, String] = Map()): BigtableSparkConfBuilder = {
    customAuthConf = CustomAuthConfig(Some(className), params)
    this
  }

  def build(): BigtableSparkConf = {
    val bigtableClientConfig = BigtableClientConfig(
      bigtableResourcesConfig = resourcesConf,
      emulatorPort = emulatorPort,
      readRowsConfig = readRowsConf,
      mutateRowsConfig = mutateRowsConf,
      batchMutateConfig = batchMutateConf,
      customAuthConfig = customAuthConf,
      userAgentConfig = userAgentConf
    )

    val applicationConfig = ApplicationConfig(
      sparkScanConfig = scanConf, sparkWritesConfig = writeConf
    )

    val errors = bigtableClientConfig.getValidationErrors() ++ applicationConfig.getValidationErrors()

    if(errors.nonEmpty) {
      throw new IllegalArgumentException(errors.mkString(System.lineSeparator()))
    }

    new BigtableSparkConf(
      bigtableClientConfig,
      applicationConfig
    )
  }
}

object BigtableSparkConfBuilder {
  def apply(): BigtableSparkConfBuilder = new BigtableSparkConfBuilder()
  private[datasources] def apply(clientConfig: BigtableClientConfig,
                                 applicationConfig: ApplicationConfig): BigtableSparkConfBuilder =
    new BigtableSparkConfBuilder(clientConfig, applicationConfig)
}

case class BigtableSparkConf private[datasources]
  (bigtableClientConfig: BigtableClientConfig,
   appConfig: ApplicationConfig) {
  def toBuilder: BigtableSparkConfBuilder =
    BigtableSparkConfBuilder(bigtableClientConfig, appConfig)
}
