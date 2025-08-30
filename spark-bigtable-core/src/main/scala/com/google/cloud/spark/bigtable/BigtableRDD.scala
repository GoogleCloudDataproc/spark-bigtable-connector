package com.google.cloud.spark.bigtable

import com.google.cloud.bigtable.data.v2.models.{RowMutationEntry, Row => BigtableRow}
import com.google.cloud.spark.bigtable.datasources.{
  BigtableDataClientBuilder,
  BigtableSparkConf,
  BigtableTableScanRDD
}
import com.google.cloud.spark.bigtable.filters.RowKeyWrapper
import com.google.common.collect.{ImmutableRangeSet, Range}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class BigtableRDD(@transient val sparkContext: SparkContext) extends Serializable with Logging {

  def readRDD(
      tableId: String,
      bigtableSparkConf: BigtableSparkConf
  ): RDD[BigtableRow] = {
    val confWithUA = getSparkConfWithUserAgent(bigtableSparkConf)
    val scanConf = confWithUA.appConfig.sparkScanConfig
    new BigtableTableScanRDD(
      confWithUA.bigtableClientConfig,
      ImmutableRangeSet.of(Range.all[RowKeyWrapper]()),
      tableId,
      sparkContext,
      scanConf.timeRangeStart,
      scanConf.timeRangeEnd,
      scanConf.maxVersions
    )
  }

  def writeRDD(
      rdd: RDD[RowMutationEntry],
      tableId: String,
      bigtableSparkConf: BigtableSparkConf
  ): Unit = {
    val bigtableClientConfig = getSparkConfWithUserAgent(bigtableSparkConf).bigtableClientConfig
    rdd
      .foreachPartition(it => {
        if (it.nonEmpty) {
          val clientHandle = BigtableDataClientBuilder.getHandle(bigtableClientConfig)
          val bigtableDataClient = clientHandle.getClient()
          val batcher = bigtableDataClient.newBulkMutationBatcher(tableId)
          it.foreach(batcher.add)
          batcher.close()
          clientHandle.close()
        }
      })
  }

  private def getSparkConfWithUserAgent(bigtableSparkConf: BigtableSparkConf): BigtableSparkConf = {
    val sparkConfBuilder = bigtableSparkConf
      .toBuilder
      .setUserAgentSourceInfo(UserAgentInformation.RDD_TEXT)
    Option(sparkContext).foreach(context =>
      sparkConfBuilder.setSparkVersion(context.version)
    )
    sparkConfBuilder.build()
  }
}
