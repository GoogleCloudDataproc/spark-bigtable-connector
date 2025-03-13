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

package spark.bigtable.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import java.nio.ByteBuffer

object WordCount extends App {
  val (projectId, instanceId, tableName, createNewTable) = parse(args)

  val spark = SparkSession.builder().getOrCreate()

  val doubleToBinaryUdf = udf((value: Double) => doubleToBinary(value))
  val binaryToDoubleUdf =
    udf((binaryData: Array[Byte]) => binaryToDouble(binaryData))

  Util.createExampleBigtable(spark, createNewTable, projectId, instanceId, tableName)
  val readDf = spark.read
    .format("bigtable")
    .option("catalog", Util.getCatalog(tableName))
    .option("spark.bigtable.project.id", projectId)
    .option("spark.bigtable.instance.id", instanceId)
    .load

  val readDfWithDouble = readDf
    .withColumn(
      "frequency_double",
      binaryToDoubleUdf(readDf.col("frequency_binary"))
    )
    .drop("frequency_binary")

  println("Reading the DataFrame from Bigtable:");
  readDfWithDouble.show()

  def parse(args: Array[String]): (String, String, String, String) = {
    import scala.util.Try
    val projectId = Try(args(0)).getOrElse {
      throw new IllegalArgumentException(
        "Missing command-line argument: SPARK_BIGTABLE_PROJECT_ID"
      )
    }
    val instanceId = Try(args(1)).getOrElse {
      throw new IllegalArgumentException(
        "Missing command-line argument: SPARK_BIGTABLE_INSTANCE_ID"
      )
    }
    val tableName = Try(args(2)).getOrElse {
      throw new IllegalArgumentException(
        "Missing command-line argument: SPARK_BIGTABLE_TABLE_NAME"
      )
    }
    val createNewTable = Try(args(3)).getOrElse {
      "true"
    }
    (projectId, instanceId, tableName, createNewTable)
  }

  def doubleToBinary(value: Double): Array[Byte] = {
    ByteBuffer.allocate(java.lang.Double.BYTES).putDouble(value).array()
  }

  def binaryToDouble(binaryData: Array[Byte]): Double = {
    if (binaryData == null || binaryData.length == 0) {
      Double.NaN
    } else {
      ByteBuffer.wrap(binaryData).getDouble()
    }
  }
}
