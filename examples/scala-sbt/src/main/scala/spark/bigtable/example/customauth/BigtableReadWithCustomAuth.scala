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

package spark.bigtable.example.customauth

import org.apache.spark.sql.SparkSession
import spark.bigtable.example.Util
import spark.bigtable.example.WordCount.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BigtableReadWithCustomAuth extends App {

  val (projectId, instanceId, tableName, createNewTable) = parse(args)

  val spark = SparkSession
    .builder()
    .appName("BigtableReadWithCustomAuth")
    .getOrCreate()

  val tokenProvider = new CustomAccessTokenProvider()
  val initialToken = tokenProvider.getCurrentToken

  Util.createExampleBigtable(spark, createNewTable, projectId, instanceId, tableName)

  tokenRefreshTest()

  try {
    val readDf = spark.read
      .format("bigtable")
      .option("catalog", Util.getCatalog(tableName))
      .option("spark.bigtable.project.id", projectId)
      .option("spark.bigtable.instance.id", instanceId)
      .option("spark.bigtable.gcp.accesstoken.provider", tokenProvider.getClass.getName)
      .load()

    println("Reading data from Bigtable...")
    readDf.show(50)
  } catch {
    case e: Exception =>
      println(s"Error reading/writing data: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    spark.stop()
  }

  private def tokenRefreshTest(): Unit = {
    Future {
      Thread.sleep(20000) // Give main thread 20s
      tokenProvider.refresh()
    }.map { _ =>
      val refreshedToken = tokenProvider.getCurrentToken
      if (initialToken != refreshedToken) {
        println("Token refresh test passed: The token has changed.")
      } else {
        println("Token refresh test failed: The token has not changed.")
      }
    }.onComplete(_ => println("Token Refresh Test completed"))
  }
}
