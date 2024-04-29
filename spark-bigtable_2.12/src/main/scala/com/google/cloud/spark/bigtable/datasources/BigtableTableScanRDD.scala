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

import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.models.Filters.{FILTERS, TimestampFilter}
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange
import com.google.cloud.bigtable.data.v2.models.{KeyOffset, Query, Row => BigtableRow}
import com.google.cloud.spark.bigtable._
import com.google.cloud.spark.bigtable.filters.RowKeyWrapper
import com.google.common.collect.{BoundType, RangeSet, TreeRangeSet, Range => GuavaRange}
import com.google.protobuf.ByteString
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.apache.yetus.audience.InterfaceAudience

import scala.collection.JavaConverters._

@InterfaceAudience.Private
class BigtableTableScanRDD(
    relation: BigtableRelation,
    clientKey: BigtableClientKey,
    filterRangeSet: RangeSet[RowKeyWrapper]
) extends RDD[BigtableRow](relation.sqlContext.sparkContext, Nil) {

  override def getPartitions: Array[Partition] = {
    try {
      val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
      val bigtableDataClient = clientHandle.getClient()

      val keyOffsets: List[KeyOffset] =
        bigtableDataClient.sampleRowKeys(relation.tableId).asScala.toList

      val tabletRanges: Array[BigtableTabletRange] =
        new Array[BigtableTabletRange](keyOffsets.size)
      // We use empty ByteString as the smallest row key and null as the largest.
      var start: ByteString = ByteString.EMPTY
      var end: ByteString = null
      var i = 0
      while (i < keyOffsets.size - 1) {
        end = keyOffsets(i).getKey
        tabletRanges(i) = BigtableTabletRange(i, start, end)
        start = end
        i += 1
      }
      // Ensure that the last tablet's 'end key' is considered null since
      //  it's considered the maximum value inside RowKeyWrapper.
      tabletRanges(i) = BigtableTabletRange(i, start, null)

      // Consider switching to using shard(List<KeyOffset> sampledRowKeys) from
      //  com.google.cloud.bigtable.data.v2.models.Query for intersecting
      //  tablet ranges with filters.
      var idx = 0
      val partitions = tabletRanges.flatMap { tabletRange =>
        val partitionRangeSet: RangeSet[RowKeyWrapper] =
          TreeRangeSet.create[RowKeyWrapper]()
        partitionRangeSet.add(
          GuavaRange.closedOpen(
            new RowKeyWrapper(tabletRange.start),
            new RowKeyWrapper(tabletRange.end)
          )
        )
        partitionRangeSet.removeAll(filterRangeSet.complement())
        if (!partitionRangeSet.isEmpty) {
          idx += 1
          Some(
            BigtableScanPartition(
              idx - 1,
              tabletRange,
              partitionRangeSet
            )
          )
        } else {
          None
        }
      }

      clientHandle.close()
      partitions.asInstanceOf[Array[Partition]]
    } catch {
      case e: Exception => {
        logError("Received error when creating partitions: " + e.getMessage)
        throw e
      }
    }
  }

  private def streamToIterator(
      stream: ServerStream[BigtableRow],
      clientHandle: BigtableDataClientBuilder.DataClientHandle
  ): Iterator[BigtableRow] = {
    val it = stream.iterator()
    val iterator = new Iterator[BigtableRow] {
      override def hasNext: Boolean = {
        if (!it.hasNext) {
          // TODO: Verify whether canceling the stream is required.
          stream.cancel()
          clientHandle.close()
          false
        } else {
          true
        }
      }
      override def next(): BigtableRow = {
        it.next()
      }
    }
    iterator
  }

  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[BigtableRow] = {
    try {
      val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
      val bigtableDataClient: BigtableDataClient = clientHandle.getClient()

      var query = Query.create(relation.tableId)
      split
        .asInstanceOf[BigtableScanPartition]
        .partitionRangeSet
        .asRanges()
        .forEach(applyRangeToQuery(_, query))
      query = addTimestampFilter(query)

      val stream: ServerStream[BigtableRow] = bigtableDataClient.readRows(query)
      streamToIterator(stream, clientHandle)
    } catch {
      case e: Exception => {
        logError(
          "Received error when reading rows from Bigtable: " + e.getMessage
        )
        throw e
      }
    }
  }

  private def applyRangeToQuery(
      guavaRange: GuavaRange[RowKeyWrapper],
      query: Query
  ): Unit = {
    if (
      guavaRange.hasLowerBound
      && guavaRange.lowerBoundType() == BoundType.CLOSED
      && guavaRange.hasUpperBound
      && guavaRange.upperBoundType() == BoundType.CLOSED
      && guavaRange.lowerEndpoint().equals(guavaRange.upperEndpoint())
    ) {
      query.rowKey(guavaRange.lowerEndpoint().getKey)
    } else {
      val byteStringRange: ByteStringRange = ByteStringRange.unbounded();

      // Handle start key
      if (guavaRange.hasLowerBound) {
        guavaRange.lowerBoundType() match {
          case BoundType.CLOSED =>
            byteStringRange.startClosed(guavaRange.lowerEndpoint().getKey)
          case BoundType.OPEN =>
            byteStringRange.startOpen(guavaRange.lowerEndpoint().getKey)
          case _ =>
            throw new IllegalArgumentException(
              "Unexpected lower bound type: " + guavaRange.lowerBoundType()
            )
        }
      }

      // Handle end key, null is considered +inf
      if (
        guavaRange.hasUpperBound && guavaRange.upperEndpoint().getKey != null
      ) {
        guavaRange.upperBoundType() match {
          case BoundType.CLOSED =>
            byteStringRange.endClosed(guavaRange.upperEndpoint().getKey)
          case BoundType.OPEN =>
            byteStringRange.endOpen(guavaRange.upperEndpoint().getKey)
          case _ =>
            throw new IllegalArgumentException(
              "Unexpected upper bound type: " + guavaRange.upperBoundType()
            )
        }
      }
      query.range(byteStringRange);
    }
  }

  private def addTimestampFilter(query: Query): Query = {
    var timestampFilter: TimestampFilter = FILTERS.timestamp()
    (relation.startTimestampMicros, relation.endTimestampMicros) match {
      case (Some(startStamp), Some(endStamp)) =>
        query.filter(
          timestampFilter.range().startClosed(startStamp).endOpen(endStamp)
        )
      case (None, Some(endStamp)) =>
        query.filter(timestampFilter.range().endOpen(endStamp))
      case (Some(startStamp), None) =>
        query.filter(timestampFilter.range().startClosed(startStamp))
      case (None, None) => query
    }
  }
}

@InterfaceAudience.Private
case class BigtableTabletRange(
    override val index: Int,
    start: ByteString,
    end: ByteString
) extends Partition

@InterfaceAudience.Private
case class BigtableScanPartition(
    override val index: Int,
    tabletRange: BigtableTabletRange,
    partitionRangeSet: RangeSet[RowKeyWrapper]
) extends Partition
