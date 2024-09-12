package com.google.cloud.spark.bigtable

import com.google.cloud.bigtable.data.v2.models.{RowMutationEntry, Row => BigtableRow}
import com.google.cloud.spark.bigtable.datasources.{BigtableClientKey, BigtableDataClientBuilder, BigtableTableScanRDD}
import com.google.cloud.spark.bigtable.filters.RowKeyWrapper
import com.google.common.collect.{ImmutableRangeSet, Range}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

class BigtableRDD(@transient val sparkContext: SparkContext) extends Serializable with Logging {

  // Overloading the method to support both Scala and Java Maps
  def readRDD(
      tableId: String,
      parameters: java.util.Map[String, String]
  ): RDD[BigtableRow] = {
    readRDD(tableId, parameters.asScala.toMap)
  }

  def readRDD(
      tableId: String,
      parameters: Map[String, String]
  ): RDD[BigtableRow] = {
    val clientKey = new BigtableClientKey(parameters)
    new BigtableTableScanRDD(
      clientKey,
      ImmutableRangeSet.of(Range.all[RowKeyWrapper]()),
      tableId,
      sparkContext,
      None,
      None
    )
  }

  // Overloading the method to support both Scala and Java Maps
  def writeRDD(
      rdd: RDD[RowMutationEntry],
      tableId: String,
      parameters: java.util.Map[String, String]
  ): Unit = {
    writeRDD(rdd, tableId, parameters.asScala.toMap)
  }

  def writeRDD(
      rdd: RDD[RowMutationEntry],
      tableId: String,
      parameters: Map[String, String]
  ): Unit = {
    val clientKey = new BigtableClientKey(parameters)
    rdd
      .foreachPartition(it => {
        if (it.nonEmpty) {
          val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
          val bigtableDataClient = clientHandle.getClient()
          val batcher = bigtableDataClient.newBulkMutationBatcher(tableId)
          it.foreach(batcher.add)
          batcher.close()
          clientHandle.close()
        }
      })
  }
}
