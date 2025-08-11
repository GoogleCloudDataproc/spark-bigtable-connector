package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.common.collect.ImmutableMap
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY

object UserAgentConfig {
  private val CONNECTOR_VERSION = "0.7.0" // ${NEXT_VERSION_FLAG}
  private val CONNECTOR_ID = "spark-bigtable"

  def apply(): UserAgentConfig = new UserAgentConfig(
    CONNECTOR_ID,
    CONNECTOR_VERSION,
    "UNSET_SPARK_VERSION",
    scala.util.Properties.versionNumberString,
    "UNSET_SOURCE"
  )
}

case class UserAgentConfig(connectorArtifactId: String,
                           connectorVersion: String,
                           sparkVersion: String,
                           scalaVersion: String,
                           sourceInfo: String) extends ClientConfigTrait {
  override def getValidationErrors: Set[String] = Set()

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    settingsBuilder
      .stubSettings()
      .setHeaderProvider(FixedHeaderProvider.create(
        ImmutableMap.of(USER_AGENT_KEY.name(), userAgentText)
      ))
  }

  def applyTableAdminSettings(settingsBuilder: BigtableTableAdminSettings.Builder): Unit = {
    settingsBuilder
      .stubSettings()
      .setHeaderProvider(FixedHeaderProvider.create(
        ImmutableMap.of(USER_AGENT_KEY.name(), userAgentText)
      ))
  }

  private def userAgentText = f"$connectorArtifactId/$connectorVersion" +
    f" spark/$sparkVersion $sourceInfo scala/$scalaVersion"

  override def debugString(): String =
    s"""UserAgentConfig(
       | connectorArtifactId: $connectorArtifactId
       | connectorVersion: $connectorVersion
       | sparkVersion: $sparkVersion
       | scalaVersion: $scalaVersion
       | sourceInfo: $sourceInfo
       |)""".stripMargin
}
