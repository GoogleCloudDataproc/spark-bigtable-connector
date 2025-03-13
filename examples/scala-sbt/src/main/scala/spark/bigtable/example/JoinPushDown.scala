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
import org.apache.spark.sql.functions.col
import java.nio.ByteBuffer

object JoinPushDown extends App {
  val (projectId, instanceId, tableName, createNewTable) = parse(args)

  implicit lazy val spark = SparkSession.builder().getOrCreate()

  val doubleToBinaryUdf = udf((value: Double) => doubleToBinary(value))
  val binaryToDoubleUdf =
    udf((binaryData: Array[Byte]) => binaryToDouble(binaryData))

  val catalog =
    s"""{
       |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
       |"rowkey":"wordCol",
       |"columns":{
       |  "word":{"cf":"rowkey", "col":"wordCol", "type":"string"},
       |  "count":{"cf":"example_family", "col":"countCol", "type":"long"},
       |  "frequency_binary":{"cf":"example_family", "col":"frequencyCol", "type":"binary"}
       |}
       |}""".stripMargin

  import spark.implicits._
  val data = (0 to 999).map(i => ("word%3d".format(i), i, i / 1000.0))
  val rdd = spark.sparkContext.parallelize(data)
  val dfWithDouble = rdd.toDF("word", "count", "frequency_double")

  println("Created the DataFrame:");
  dfWithDouble.show()

  val dfToWrite = dfWithDouble
    .withColumn(
      "frequency_binary",
      doubleToBinaryUdf(dfWithDouble.col("frequency_double"))
    )
    .drop("frequency_double")

  dfToWrite.write
    .format("bigtable")
    .option("catalog", catalog)
    .option("spark.bigtable.project.id", projectId)
    .option("spark.bigtable.instance.id", instanceId)
    .option("spark.bigtable.create.new.table", createNewTable)
    .save
  println("DataFrame was written to Bigtable.")

  val srcDf = dfToWrite.sample(100/1000).selectExpr("word", "count as src_count")

  private val joinConfig: Map[String, String] = Map(
    "spark.bigtable.project.id" -> projectId,
    "spark.bigtable.instance.id" -> instanceId,
    "catalog" -> catalog,
    "join.type" -> "inner",
    "columns.required" -> "word,src_count,count",
    "partition.count" -> "10",
    "batch.rowKeySize" -> "100",
    "alias.name" -> "bt"
  )

  import com.google.cloud.spark.bigtable.join.BigtableJoinImplicit._
  val resDf = srcDf.joinWithBigtable(joinConfig, "word", Seq("word"))

  println("Pushed down join output:");
  resDf.show()

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
