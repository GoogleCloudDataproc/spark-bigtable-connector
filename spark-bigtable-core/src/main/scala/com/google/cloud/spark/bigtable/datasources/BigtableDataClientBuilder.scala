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

import com.google.api.gax.batching.{BatchingSettings, FlowControlSettings}
import com.google.api.gax.core.CredentialsProvider
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.bigtable.admin.v2.{BigtableTableAdminClient, BigtableTableAdminSettings}
import com.google.cloud.bigtable.data.v2.{BigtableDataClient, BigtableDataSettings}
import com.google.cloud.spark.bigtable._
import com.google.common.collect.ImmutableMap
import com.google.cloud.spark.bigtable.util.Reflector
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY
import org.apache.yetus.audience.InterfaceAudience
import org.threeten.bp.Duration

import scala.collection.mutable
import scala.math.max
import scala.util.{Failure, Success}

/** This class is responsible for creating BigtableDataClient objects, setting
  * appropriate runtime configurations, and cashing them per each Spark worker
  * to improve performance.
  */
@InterfaceAudience.Private
object BigtableDataClientBuilder extends Serializable with Logging {

  class DataClientHandle(
      client: BigtableDataClient,
      clientKey: BigtableClientKey
  ) extends AutoCloseable {
    private var wasReleased: Boolean = false

    def getClient(): BigtableDataClient = {
      client
    }

    override def close(): Unit = {
      if (!wasReleased) {
        wasReleased = true
        releaseDataClient(clientKey)
      } else {
        logWarning(
          "Calling close() more than once on handle with key = " + clientKey
        )
      }
    }
  }

  private val dataClientMap =
    mutable.HashMap[BigtableClientKey, BigtableDataClient]()
  private val refCountMap = mutable.HashMap[BigtableClientKey, Int]()

  def getHandle(clientKey: BigtableClientKey): DataClientHandle = synchronized {
    if (!refCountMap.contains(clientKey)) {
      refCountMap += (clientKey -> 0)
      logDebug("Creating a new BigtableDataClient with key = " + clientKey)
      dataClientMap += (clientKey -> createDataClient(clientKey))
    }
    refCountMap(clientKey) += 1
    new DataClientHandle(dataClientMap(clientKey), clientKey)
  }

  private def releaseDataClient(clientKey: BigtableClientKey): Unit =
    synchronized {
      if (!refCountMap.contains(clientKey)) {
        logError("Release was called without an open Data Client.")
      } else {
        refCountMap(clientKey) -= 1
        if (refCountMap(clientKey) == 0) {
          refCountMap.remove(clientKey)
          dataClientMap.get(clientKey).foreach(dataClient => dataClient.close())
          dataClientMap.remove(clientKey)
        }
      }
    }

  private def createDataClient(
      clientKey: BigtableClientKey
  ): BigtableDataClient = {
    val settingsBuilder: BigtableDataSettings.Builder = createBuilder(clientKey)
    configureBigtableResource(clientKey, settingsBuilder)
    configureHeaderProvider(clientKey, settingsBuilder)
    configureWriteSettings(clientKey, settingsBuilder)
    configureReadSettings(clientKey, settingsBuilder)

    BigtableDataClient.create(settingsBuilder.build())
  }

  private def createBuilder(
      clientKey: BigtableClientKey
  ): BigtableDataSettings.Builder = {
    clientKey.emulatorPort match {
      case None       => BigtableDataSettings.newBuilder()
      case Some(port) => BigtableDataSettings.newBuilderForEmulator(port)
    }
  }

  private def configureBigtableResource(
      clientKey: BigtableClientKey,
      settingsBuilder: BigtableDataSettings.Builder
  ): Unit = {
    settingsBuilder
      .setProjectId(clientKey.projectId)
      .setInstanceId(clientKey.instanceId)
      .setAppProfileId(clientKey.appProfileId)

    clientKey.customCredentialsProviderFQCN match {
      case Some(credentialsProviderFQCN) =>
        logInfo(s"Authentication using custom credential provider: $credentialsProviderFQCN")

        // First try a constructor with parameters, then an empty constructor
        Reflector
          .createVerifiedInstance[CredentialsProvider](credentialsProviderFQCN, clientKey.customCredentialsProviderArgs)
          .orElse(Reflector.createVerifiedInstance[CredentialsProvider](credentialsProviderFQCN)) match {
          case Success(provider) => settingsBuilder.setCredentialsProvider(provider)
          case Failure(e) => throw e
        }
      case None => logInfo(s"Authentication using default credential provider")
    }
  }

  private def configureHeaderProvider(
      clientKey: BigtableClientKey,
      settingsBuilder: BigtableDataSettings.Builder
  ): Unit = {
    val headersBuilder: ImmutableMap.Builder[String, String] =
      ImmutableMap.builder()
    headersBuilder.put(USER_AGENT_KEY.name(), clientKey.userAgentText)
    settingsBuilder
      .stubSettings()
      .setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()))
  }

  private def configureWriteSettings(
      clientKey: BigtableClientKey,
      settingsBuilder: BigtableDataSettings.Builder
  ): Unit = {
    settingsBuilder.setBulkMutationFlowControl(clientKey.batchMutateFlowControl)

    val batchSize = clientKey.batchSize
    val maxBatchSize = clientKey.maxBatchSize

    if (batchSize < 1 || batchSize > maxBatchSize) {
      throw new IllegalArgumentException(
        "Parameter "
          + BigtableSparkConf.BIGTABLE_BATCH_MUTATE_SIZE
          + " must be between 1 and "
          + maxBatchSize
          + "."
      )
    }
    val defaultBatchingSettings: BatchingSettings = settingsBuilder
      .stubSettings()
      .bulkMutateRowsSettings()
      .getBatchingSettings()
    val newBatchingSettingsBuilder: BatchingSettings.Builder =
      defaultBatchingSettings
        .toBuilder()
        .setElementCountThreshold(batchSize)

    // maxOutstandingElementCount from Flow Control should be larger than the batch size.
    val defaultFlowControlSettings: FlowControlSettings =
      defaultBatchingSettings.getFlowControlSettings()
    val defaultMaxOutstandingElementCount: Long =
      defaultFlowControlSettings.getMaxOutstandingElementCount()
    val newMaxOutstandingElementCount: Long =
      max(defaultMaxOutstandingElementCount, batchSize + 1)
    newBatchingSettingsBuilder.setFlowControlSettings(
      defaultFlowControlSettings
        .toBuilder()
        .setMaxOutstandingElementCount(newMaxOutstandingElementCount)
        .build()
    )

    settingsBuilder
      .stubSettings()
      .bulkMutateRowsSettings()
      .setBatchingSettings(newBatchingSettingsBuilder.build())

    clientKey.mutateRowsAttemptTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .bulkMutateRowsSettings()
        .retrySettings()
        .setInitialRpcTimeout(Duration.ofMillis(timeout.toLong))
    )
    clientKey.mutateRowsTotalTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .bulkMutateRowsSettings()
        .retrySettings()
        .setTotalTimeout(Duration.ofMillis(timeout.toLong))
    )
  }

  private def configureReadSettings(
      clientKey: BigtableClientKey,
      settingsBuilder: BigtableDataSettings.Builder
  ): Unit = {
    clientKey.readRowsAttemptTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setInitialRpcTimeout(Duration.ofMillis(timeout.toLong))
    )
    clientKey.readRowsTotalTimeout.foreach(timeout =>
      settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setTotalTimeout(Duration.ofMillis(timeout.toLong))
    )

    clientKey.readRowsRetries.foreach(retries =>
      settingsBuilder
        .stubSettings()
        .readRowsSettings()
        .retrySettings()
        .setMaxAttempts(retries.toInt)
    )
  }
}
/** This class is responsible for creating BigtableAdminClient objects and
  * setting appropriate runtime configurations.
  */
@InterfaceAudience.Private
object BigtableAdminClientBuilder extends Serializable {
  def getAdminClient(clientKey: BigtableClientKey): BigtableTableAdminClient = {
    // We don't cache the admin client since it's created infrequently.
    createAdminClient(clientKey)
  }

  private def createAdminClient(
      clientKey: BigtableClientKey
  ): BigtableTableAdminClient = {
    val settingsBuilder: BigtableTableAdminSettings.Builder = {
      clientKey.emulatorPort match {
        case None => BigtableTableAdminSettings.newBuilder()
        case Some(port) =>
          BigtableTableAdminSettings.newBuilderForEmulator(port)
      }
    }.setProjectId(clientKey.projectId)
      .setInstanceId(clientKey.instanceId)

    addUserAgent(clientKey, settingsBuilder)

    BigtableTableAdminClient.create(settingsBuilder.build())
  }

  private def addUserAgent(
      clientKey: BigtableClientKey,
      settingsBuilder: BigtableTableAdminSettings.Builder
  ): Unit = {
    val headersBuilder: ImmutableMap.Builder[String, String] =
      ImmutableMap.builder()
    headersBuilder.put(USER_AGENT_KEY.name(), clientKey.userAgentText)
    settingsBuilder
      .stubSettings()
      .setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()))
  }
}

@InterfaceAudience.Private
class BigtableClientKey(
    @transient bigtableSparkConf: BigtableSparkConf,
    dataFrameOrRDDUserAgentText: String
) extends Serializable {
  val projectId: String = bigtableSparkConf.projectId.getOrElse(
    throw new IllegalArgumentException(
      "Parameter " +
        BigtableSparkConf.BIGTABLE_PROJECT_ID + " must be provided."
    )
  )
  val instanceId: String = bigtableSparkConf.instanceId.getOrElse(
    throw new IllegalArgumentException(
      "Parameter " +
        BigtableSparkConf.BIGTABLE_INSTANCE_ID + " must be provided."
    )
  )

  val appProfileId: String = bigtableSparkConf.appProfileId
  val emulatorPort: Option[Int] = bigtableSparkConf.emulatorPort

  val batchMutateFlowControl: Boolean = bigtableSparkConf.enableBatchMutateFlowControl

  val readRowsAttemptTimeout: Option[String] = bigtableSparkConf.readRowsAttemptTimeoutMs
  val readRowsTotalTimeout: Option[String] = bigtableSparkConf.readRowsTotalTimeoutMs

  val mutateRowsAttemptTimeout: Option[String] =
    bigtableSparkConf.mutateRowsAttemptTimeoutMs
  val mutateRowsTotalTimeout: Option[String] = bigtableSparkConf.mutateRowsTotalTimeoutMs

  val readRowsRetries: Option[String] = bigtableSparkConf.maxReadRowsRetries

  val maxBatchSize: Long = BigtableSparkConf.BIGTABLE_MAX_BATCH_MUTATE_SIZE
  val batchSize: Long = bigtableSparkConf.batchMutateSize
  val customCredentialsProviderFQCN: Option[String] =
    bigtableSparkConf.customCredentialsProviderFQCN
  val customCredentialsProviderArgs: Map[String, String] =
    bigtableSparkConf.customCredentialsProviderArgs

  val userAgentText: String =
    ("spark-bigtable_2.12/" + UserAgentInformation.CONNECTOR_VERSION
      + " spark/" + UserAgentInformation.sparkVersion
      + " " + dataFrameOrRDDUserAgentText
      + " scala/" + UserAgentInformation.scalaVersion)

  override def hashCode: Int = {
    val prime: Int = 31
    var result: Int = 1

    result = prime * result + projectId.hashCode
    result = prime * result + instanceId.hashCode
    result = prime * result + appProfileId.hashCode
    result = prime * result + emulatorPort.hashCode
    result = prime * result + batchMutateFlowControl.hashCode
    result = prime * result + readRowsAttemptTimeout.getOrElse("").hashCode
    result = prime * result + readRowsTotalTimeout.getOrElse("").hashCode
    result = prime * result + mutateRowsAttemptTimeout.getOrElse("").hashCode
    result = prime * result + mutateRowsTotalTimeout.getOrElse("").hashCode
    result = prime * result + readRowsRetries.getOrElse("").hashCode
    result = prime * result + batchSize.hashCode
    result = prime * result + customCredentialsProviderFQCN.getOrElse("").hashCode
    result = prime * result + customCredentialsProviderArgs.hashCode

    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val that: BigtableClientKey = obj.asInstanceOf[BigtableClientKey]
    if (
      this.projectId != that.projectId ||
      this.instanceId != that.instanceId ||
      this.appProfileId != that.appProfileId ||
      this.emulatorPort != that.emulatorPort ||
      this.batchMutateFlowControl != that.batchMutateFlowControl ||
      this.readRowsAttemptTimeout != that.readRowsAttemptTimeout ||
      this.readRowsTotalTimeout != that.readRowsTotalTimeout ||
      this.mutateRowsAttemptTimeout != that.mutateRowsAttemptTimeout ||
      this.mutateRowsTotalTimeout != that.mutateRowsTotalTimeout ||
      this.readRowsRetries != that.readRowsRetries ||
      this.batchSize != that.batchSize ||
      this.customCredentialsProviderFQCN != that.customCredentialsProviderFQCN ||
      this.customCredentialsProviderArgs != that.customCredentialsProviderArgs
    ) {
      return false
    }
    true
  }

  override def toString(): String = {
    s"""BigtableClientKey(
        | projectId = $projectId,
        | instanceId = $instanceId,
        | appProfileId = $appProfileId,
        | emulatorPort = $emulatorPort,
        | batchMutateFlowControl = $batchMutateFlowControl,
        | readRowsAttemptTimeout = $readRowsAttemptTimeout,
        | readRowsTotalTimeout = $readRowsTotalTimeout,
        | mutateRowsAttemptTimeout = $mutateRowsAttemptTimeout,
        | mutateRowsTotalTimeout = $mutateRowsTotalTimeout,
        | batchSize = $batchSize,
        | userAgentText = $userAgentText,
        | customCredentialsProviderFQCN = $customCredentialsProviderFQCN
        |)""".stripMargin.replaceAll("\n", " ")
  }
}
