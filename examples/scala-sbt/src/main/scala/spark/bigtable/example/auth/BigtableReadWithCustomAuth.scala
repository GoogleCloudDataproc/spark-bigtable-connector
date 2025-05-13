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

package spark.bigtable.example.auth

import org.apache.spark.sql.SparkSession
import spark.bigtable.example.Util
import spark.bigtable.example.WordCount.parse

object BigtableReadWithCustomAuth extends App {

  val (projectId, instanceId, tableName, createNewTable) = parse(args)

  val spark = SparkSession
    .builder()
    .appName("BigtableReadWithCustomAuth")
    .getOrCreate()

  val credentilasProvider = new CustomCredentilasProvider()

  Util.createExampleBigtable(spark, createNewTable, projectId, instanceId, tableName)

  try {
    val readDf = spark.read
      .format("bigtable")
      .option("catalog", Util.getCatalog(tableName))
      .option("spark.bigtable.project.id", projectId)
      .option("spark.bigtable.instance.id", instanceId)
      .option("spark.bigtable.auth.credentials_provider", credentilasProvider.getClass.getName)
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
}
