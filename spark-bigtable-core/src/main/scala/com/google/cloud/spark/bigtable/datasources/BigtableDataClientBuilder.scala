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

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.spark.bigtable._
import com.google.cloud.spark.bigtable.datasources.config.BigtableClientConfig
import org.apache.yetus.audience.InterfaceAudience

import scala.collection.mutable

/** This class is responsible for creating BigtableDataClient objects, setting
  * appropriate runtime configurations, and cashing them per each Spark worker
  * to improve performance.
  */
@InterfaceAudience.Private
object BigtableDataClientBuilder extends Serializable with Logging {

  class DataClientHandle(
                          client: BigtableDataClient,
                          clientConfig: BigtableClientConfig
                        ) extends AutoCloseable {
    private var wasReleased: Boolean = false

    def getClient(): BigtableDataClient = {
      client
    }

    override def close(): Unit = {
      if (!wasReleased) {
        wasReleased = true
        releaseDataClient(clientConfig)
      } else {
        logWarning(
          "Calling close() more than once on handle with key = " + clientConfig
        )
      }
    }
  }

  private val dataClientMap =
    mutable.HashMap[BigtableClientConfig, BigtableDataClient]()
  private val refCountMap = mutable.HashMap[BigtableClientConfig, Int]()

  def getHandle(clientConfig: BigtableClientConfig): DataClientHandle = synchronized {
    if (!refCountMap.contains(clientConfig)) {
      refCountMap += (clientConfig -> 0)
      logDebug("Creating a new BigtableDataClient with key = " + clientConfig)
      dataClientMap += (clientConfig -> BigtableDataClient.create(clientConfig.getDataSettings()))
    }
    refCountMap(clientConfig) += 1
    new DataClientHandle(dataClientMap(clientConfig), clientConfig)
  }

  private def releaseDataClient(clientConfig: BigtableClientConfig): Unit =
    synchronized {
      if (!refCountMap.contains(clientConfig)) {
        logError("Release was called without an open Data Client.")
      } else {
        refCountMap(clientConfig) -= 1
        if (refCountMap(clientConfig) == 0) {
          refCountMap.remove(clientConfig)
          dataClientMap.get(clientConfig).foreach(dataClient => dataClient.close())
          dataClientMap.remove(clientConfig)
        }
      }
    }
}
/** This class is responsible for creating BigtableAdminClient objects and
  * setting appropriate runtime configurations.
  */
@InterfaceAudience.Private
object BigtableAdminClientBuilder extends Serializable {
  def getAdminClient(clientConfig: BigtableClientConfig): BigtableTableAdminClient = {
    // We don't cache the admin client since it's created infrequently.
    createAdminClient(clientConfig)
  }

  private def createAdminClient(
      clientConfig: BigtableClientConfig
  ): BigtableTableAdminClient =
    BigtableTableAdminClient.create(clientConfig.getTableAdminSettings())
}

