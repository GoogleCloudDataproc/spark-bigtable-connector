/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spark.bigtable.Logging
import com.google.cloud.spark.bigtable.datasources.BigtableSparkConfBuilder
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY
import org.scalatest.funsuite.AnyFunSuite

class UserAgentConfigTest extends AnyFunSuite with Logging {

  // Verify that the default UserAgentConfig constructor builds a valid, non-empty user-agent string without nulls.
  test("UserAgentConfig handles default values without null errors") {
    val config = UserAgentConfig()
    val text = config.userAgentText

    assert(text != null && text.nonEmpty)
    assert(!text.contains("null"))
    assert(!text.contains("  "))
    assert(text.startsWith("spark-bigtable/"))
  }

  // Verify null-safety: passing null or empty fields must not throw NPEs or format "null" into the string.
  test("UserAgentConfig handles null and empty fields gracefully") {
    val configWithNulls = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = null,
      sourceInfo = null,
      platformOrRuntime = null
    )
    val text = configWithNulls.userAgentText

    assert(text != null && text.nonEmpty)
    assert(!text.contains("null"))
    assert(!text.contains("  "))
    assert(text.trim == text)
  }

  // Verify that the detected platform/runtime flag is included only when populated.
  test("UserAgentConfig conditionally includes the platform or runtime flag") {
    val configWithDataproc = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      platformOrRuntime = "dataproc/3.0"
    )
    assert(configWithDataproc.userAgentText.contains("dataproc/3.0"))

    val configWithGke = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      platformOrRuntime = "platform/gke"
    )
    assert(configWithGke.userAgentText.contains("platform/gke"))

    val configWithDatabricks = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      platformOrRuntime = "databricks/18.3"
    )
    assert(configWithDatabricks.userAgentText.contains("databricks/18.3"))

    val configWithoutPlatform = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      platformOrRuntime = ""
    )
    assert(configWithoutPlatform.userAgentText == "spark-bigtable/0.10.1 spark/3.5.1 DF/V1 scala/2.12.18")
  }

  // Verify that UserAgentConfig properly configures FixedHeaderProvider on BigtableDataSettings.
  test("UserAgentConfig applies user-agent header to BigtableDataSettings") {
    val config = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      platformOrRuntime = "dataproc/3.0"
    )

    val settingsBuilder = BigtableDataSettings.newBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
    config.applySettings(settingsBuilder)

    val headerProvider = settingsBuilder.stubSettings().getHeaderProvider
    val headers = headerProvider.getHeaders
    assert(headers.containsKey(USER_AGENT_KEY.name()))
    val headerVal = headers.get(USER_AGENT_KEY.name())
    assert(headerVal != null && headerVal.contains("dataproc/3.0"))
  }

  // Verify that UserAgentConfig properly configures FixedHeaderProvider on BigtableTableAdminSettings.
  test("UserAgentConfig applies user-agent header to BigtableTableAdminSettings") {
    val config = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      platformOrRuntime = "dataproc/3.0"
    )

    val settingsBuilder = BigtableTableAdminSettings.newBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
    config.applyTableAdminSettings(settingsBuilder)

    val headerProvider = settingsBuilder.stubSettings().getHeaderProvider
    val headers = headerProvider.getHeaders
    assert(headers.containsKey(USER_AGENT_KEY.name()))
    val headerVal = headers.get(USER_AGENT_KEY.name())
    assert(headerVal != null && headerVal.contains("dataproc/3.0"))
  }

  // Verify that BigtableSparkConfBuilder setters correctly populate UserAgentConfig fields and output.
  test("BigtableSparkConfBuilder configures UserAgentConfig without errors") {
    val conf = BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .setSparkVersion("3.5.1")
      .setUserAgentSourceInfo("DF/V1")
      .build()

    val userAgentConfig = conf.bigtableClientConfig.userAgentConfig
    assert(userAgentConfig.sparkVersion == "3.5.1")
    assert(userAgentConfig.sourceInfo == "DF/V1")
    assert(userAgentConfig.userAgentText.startsWith("spark-bigtable/"))
    assert(userAgentConfig.userAgentText.contains("spark/3.5.1"))
    assert(userAgentConfig.userAgentText.contains("DF/V1"))

    // platformOrRuntime has no setter: it is pulled from the UserAgentConfig default
    // argument when the builder is constructed. Assert it survives the builder's
    // copy chain into the built conf and is rendered as the trailing token.
    // Kept environment-agnostic so this holds whether or not a platform is detected.
    val expectedPlatform = UserAgentConfig.PLATFORM_OR_RUNTIME
    assert(userAgentConfig.platformOrRuntime == expectedPlatform)
    if (expectedPlatform.nonEmpty) {
      assert(userAgentConfig.userAgentText.endsWith(expectedPlatform))
    } else {
      assert(userAgentConfig.userAgentText.endsWith(s"scala/${userAgentConfig.scalaVersion}"))
    }
  }

  // Verify that a populated platformOrRuntime survives the builder's copy chain.
  // Seeds a known value rather than relying on UserAgentConfig.PLATFORM_OR_RUNTIME,
  // which is empty on a developer laptop and would make the assertion vacuous.
  test("platformOrRuntime survives a toBuilder round-trip and later setters") {
    val base = BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .setSparkVersion("3.5.1")
      .setUserAgentSourceInfo("DF/V1")
      .build()

    val seeded = base.copy(
      bigtableClientConfig = base.bigtableClientConfig.copy(
        userAgentConfig = base.bigtableClientConfig.userAgentConfig.copy(
          platformOrRuntime = "dataproc/3.0"
        )
      )
    )

    // toBuilder -> setter -> build() is the exact path BigtableRDD uses when it
    // overwrites sourceInfo with RDD_TEXT, so a clobbered field would surface here.
    val roundTripped = seeded.toBuilder
      .setUserAgentSourceInfo("RDD/")
      .build()

    val ua = roundTripped.bigtableClientConfig.userAgentConfig
    assert(ua.platformOrRuntime == "dataproc/3.0")
    assert(ua.sourceInfo == "RDD/")
    assert(ua.sparkVersion == "3.5.1")
    assert(ua.userAgentText.endsWith("dataproc/3.0"))
  }

  // Verify that companion object detection constants are defined and non-null.
  test("PLATFORM_OR_RUNTIME is defined") {
    assert(UserAgentConfig.PLATFORM_OR_RUNTIME != null)
    assert(UserAgentConfig.DETECTED_PLATFORM_OR_RUNTIME
      .forall(flag => UserAgentConfig.PLATFORM_OR_RUNTIME == flag.flag))
  }

  // Verify that isMsasServerless detects hostname with gdpic prefix.
  test("isMsasServerless identifies gdpic prefix on hostname") {
    assert(UserAgentConfig.isMsasServerless(Some("gdpic-batch-1234-w-0")))
    assert(UserAgentConfig.isMsasServerless(Some("gdpic1234")))
    assert(UserAgentConfig.isMsasServerless(Some("gdpic-driver")))
    assert(!UserAgentConfig.isMsasServerless(Some("cluster-7f3a-w-0")))
    assert(!UserAgentConfig.isMsasServerless(Some("spark-k8s-pod")))
    assert(!UserAgentConfig.isMsasServerless(None))
  }

  // Verify that UserAgentFlag types produce expected flags for GCP runtimes and other platforms.
  test("UserAgentFlag produces correct formatted flags") {
    // GCP runtimes with versions
    assert(GcpRuntime.Dataproc("3.0").flag == "dataproc/3.0")

    // Platform flags (platform/<name>)
    assert(Platform.GcpServerless.flag == "platform/gcp-serverless")
    assert(Platform.GKE.flag == "platform/gke")
    assert(Platform.EKS.flag == "platform/eks")
    assert(Platform.EMR.flag == "platform/emr")
    assert(Platform.EMRServerless.flag == "platform/emr-serverless")
    assert(Platform.K8s.flag == "platform/k8s")

    // Databricks runtime
    assert(Platform.Databricks("18.3").flag == "databricks/18.3")
    assert(Platform.Databricks("  18.3  ").flag == "databricks/18.3")
    assert(Platform.Databricks("").flag == "databricks")
    assert(Platform.Databricks(null).flag == "databricks")
  }

  // Verify priority chain in detectPlatformOrRuntime
  test("detectPlatformOrRuntime strictly follows priority chain") {
    // 1. Dataproc on GCE takes top priority
    val gceEnv: String => Option[String] = Map(
      "DATAPROC_IMAGE_VERSION" -> "3.0",
      "PLATFORM_TYPE" -> "EMR_SERVERLESS",
      "spark.emr.default.executor.cores" -> "4",
      "DATABRICKS_RUNTIME_VERSION" -> "18.3"
    ).get
    val gceDetected = UserAgentConfig.detectPlatformOrRuntime(gceEnv, Some("cluster-7f3a-m"), isK8s = true)
    assert(gceDetected.contains(GcpRuntime.Dataproc("3.0")))

    // 2. Dataproc Serverless (gdpic hostname) takes precedence over EMR, Databricks, K8s
    val serverlessEnv: String => Option[String] = Map(
      "PLATFORM_TYPE" -> "EMR_SERVERLESS",
      "spark.emr.default.executor.cores" -> "4",
      "DATABRICKS_RUNTIME_VERSION" -> "18.3"
    ).get
    val serverlessDetected = UserAgentConfig.detectPlatformOrRuntime(serverlessEnv, Some("gdpic-batch-1-m"), isK8s = true)
    assert(serverlessDetected.contains(Platform.GcpServerless))

    // 3. EMR Serverless takes precedence over EMR EC2, Databricks, K8s
    val emrServerlessEnv: String => Option[String] = Map(
      "PLATFORM_TYPE" -> "EMR_SERVERLESS",
      "spark.emr.default.executor.cores" -> "4",
      "DATABRICKS_RUNTIME_VERSION" -> "18.3"
    ).get
    val emrServerlessDetected = UserAgentConfig.detectPlatformOrRuntime(emrServerlessEnv, Some("worker-1"), isK8s = true)
    assert(emrServerlessDetected.contains(Platform.EMRServerless))

    // 4. EMR on EC2 (isK8s = false) takes precedence over Databricks
    val emrEnv: String => Option[String] = Map(
      "spark.emr.default.executor.cores" -> "4",
      "DATABRICKS_RUNTIME_VERSION" -> "18.3"
    ).get
    val emrDetected = UserAgentConfig.detectPlatformOrRuntime(emrEnv, Some("ip-10-0-0-1"), isK8s = false)
    assert(emrDetected.contains(Platform.EMR))

    // 4b. EMR on EKS (AWS extensions set and isK8s = true) detects Platform.EKS
    val eksEnv: String => Option[String] = Map(
      "spark.sql.emr.internal.extensions" -> "com.amazonaws.emr.spark.EmrSparkSessionExtensions",
      "DATABRICKS_RUNTIME_VERSION" -> "18.3"
    ).get
    val eksDetected = UserAgentConfig.detectPlatformOrRuntime(eksEnv, Some("spark-eks-pod"), isK8s = true)
    assert(eksDetected.contains(Platform.EKS))

    // 5. Databricks takes precedence over K8s
    val dbrEnv: String => Option[String] = Map(
      "DATABRICKS_RUNTIME_VERSION" -> "18.3"
    ).get
    val dbrDetected = UserAgentConfig.detectPlatformOrRuntime(dbrEnv, Some("worker-1"), isK8s = true)
    assert(dbrDetected.contains(Platform.Databricks("18.3")))

    // 6. Kubernetes defaults to Platform.K8s when no sub-platform is specified
    val k8sEnv: String => Option[String] = Map[String, String]().get
    val k8sDetected = UserAgentConfig.detectPlatformOrRuntime(k8sEnv, Some("spark-driver-pod"), isK8s = true)
    assert(k8sDetected.contains(Platform.K8s))

    // 7b. Kubernetes auto-detects GKE via DATAPROC_DIR or spark.kubernetes.container.image
    val gkeAutoEnv1: String => Option[String] = Map("DATAPROC_DIR" -> "/dataproc").get
    assert(UserAgentConfig.detectPlatformOrRuntime(gkeAutoEnv1, Some("spark-driver-pod"), isK8s = true).contains(Platform.GKE))

    val gkeAutoEnv2: String => Option[String] = Map(
      "spark.kubernetes.container.image" -> "us-west1-docker.pkg.dev/cloud-dataproc/spark/dataproc_2.2:3.5-dataproc-28"
    ).get
    assert(UserAgentConfig.detectPlatformOrRuntime(gkeAutoEnv2, Some("spark-driver-pod"), isK8s = true).contains(Platform.GKE))

    val gkeAutoEnv3: String => Option[String] = Map(
      "spark.kubernetes.container.image" -> "docker.io/custom/dataproc-spark:3.5"
    ).get
    assert(UserAgentConfig.detectPlatformOrRuntime(gkeAutoEnv3, Some("spark-driver-pod"), isK8s = true).contains(Platform.GKE))

    val nonGkeK8sEnv: String => Option[String] = Map(
      "spark.kubernetes.container.image" -> "us-docker.pkg.dev/my-project/my-spark:latest"
    ).get
    assert(UserAgentConfig.detectPlatformOrRuntime(nonGkeK8sEnv, Some("spark-driver-pod"), isK8s = true).contains(Platform.K8s))

    // 8. When nothing is detected
    val noneDetected = UserAgentConfig.detectPlatformOrRuntime(k8sEnv, Some("my-laptop"), isK8s = false)
    assert(noneDetected.isEmpty)
  }

  // Verify that EMR and EMR Serverless runtime indicators are accurately detected
  test("detectPlatformOrRuntime detects EMR Serverless via runtime properties") {
    // Via PLATFORM_TYPE
    val env1: String => Option[String] = Map("PLATFORM_TYPE" -> "EMR_SERVERLESS").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env1, None, isK8s = false).contains(Platform.EMRServerless))

    // Via SERVERLESS_EMR_JOB_ID
    val env2: String => Option[String] = Map("SERVERLESS_EMR_JOB_ID" -> "00g8koeifiof2g0n").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env2, None, isK8s = false).contains(Platform.EMRServerless))

    // Via spark.master (both "custom:emr-serverless" and "emr-serverless")
    val env3: String => Option[String] = Map("spark.master" -> "custom:emr-serverless").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env3, None, isK8s = false).contains(Platform.EMRServerless))

    val env4: String => Option[String] = Map("spark.master" -> "emr-serverless").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env4, None, isK8s = false).contains(Platform.EMRServerless))
  }

  test("detectPlatformOrRuntime detects EMR on EC2 via runtime properties") {
    // Via spark.emr.default.executor.cores
    val env1: String => Option[String] = Map("spark.emr.default.executor.cores" -> "4").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env1, None, isK8s = false).contains(Platform.EMR))

    // Via spark.sql.emr.internal.extensions
    val env2: String => Option[String] = Map("spark.sql.emr.internal.extensions" -> "com.amazonaws.emr.spark.EmrSparkSessionExtensions").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env2, None, isK8s = false).contains(Platform.EMR))
  }

  test("detectPlatformOrRuntime detects EMR on EKS via runtime properties") {
    // Via spark.sql.emr.internal.extensions inside Kubernetes
    val env: String => Option[String] = Map("spark.sql.emr.internal.extensions" -> "com.amazonaws.emr.spark.EmrSparkSessionExtensions").get
    assert(UserAgentConfig.detectPlatformOrRuntime(env, None, isK8s = true).contains(Platform.EKS))
  }

  test("isAwsPlatform recognizes AWS platform properties") {
    assert(UserAgentConfig.isAwsPlatform(Map("spark.sql.emr.internal.extensions" -> "com.amazonaws.emr.spark.EmrSparkSessionExtensions").get))
    assert(UserAgentConfig.isAwsPlatform(Map("spark.emr.default.executor.cores" -> "4").get))
    assert(UserAgentConfig.isAwsPlatform(Map("PLATFORM_TYPE" -> "EMR_SERVERLESS").get))
    assert(UserAgentConfig.isAwsPlatform(Map("spark.master" -> "custom:emr-serverless").get))
    assert(UserAgentConfig.isAwsPlatform(Map("SERVERLESS_EMR_JOB_ID" -> "job-123").get))
    assert(!UserAgentConfig.isAwsPlatform(Map("DATAPROC_IMAGE_VERSION" -> "2.2").get))
    assert(!UserAgentConfig.isAwsPlatform(Map[String, String]().get))
  }

  test("detectPlatformOrRuntime progressively escapes when GCP platform is detected") {
    // Even if EMR or Serverless properties are set, GCP detection takes precedence and escapes early
    var emrChecked = false
    val env: String => Option[String] = {
      case "DATAPROC_IMAGE_VERSION" => Some("2.2")
      case "PLATFORM_TYPE" =>
        emrChecked = true
        Some("EMR_SERVERLESS")
      case "spark.emr.default.executor.cores" =>
        emrChecked = true
        Some("4")
      case _ => None
    }
    val detected = UserAgentConfig.detectPlatformOrRuntime(env, None, isK8s = false)
    assert(detected.contains(GcpRuntime.Dataproc("2.2")))
    assert(!emrChecked, "Non-GCP properties must not be queried when a GCP platform is identified")
  }
}
