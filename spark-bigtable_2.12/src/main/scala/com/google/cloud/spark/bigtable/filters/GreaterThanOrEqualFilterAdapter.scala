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

package com.google.cloud.spark.bigtable.filters

import com.google.cloud.spark.bigtable.datasources.BytesConverter
import com.google.common.collect.{ImmutableRangeSet, Range, RangeSet, TreeRangeSet}
import org.apache.spark.unsafe.types.UTF8String

object GreaterThanOrEqualFilterAdapter {
  def convertValueToRangeSet(value: Any): RangeSet[RowKeyWrapper] =
    value match {
      // Because of how long byte encoding uses two's complement, we have:
      // bytes(0L) < bytes(MAX_LONG) < bytes(MIN_LONG) < bytes(-1L)
      case typedVal: Long =>
        val valueBytes: Array[Byte] = BytesConverter.toBytes(typedVal)
        val rangeSetResult: RangeSet[RowKeyWrapper] =
          TreeRangeSet.create[RowKeyWrapper]()
        if (typedVal >= 0) {
          rangeSetResult.add(
            Range.closed(
              new RowKeyWrapper(valueBytes),
              new RowKeyWrapper(BytesConverter.toBytes(Long.MaxValue))
            )
          )
        } else {
          rangeSetResult.add(
            Range.closed(
              new RowKeyWrapper(valueBytes),
              new RowKeyWrapper(BytesConverter.toBytes(-1L: Long))
            )
          )
          rangeSetResult.add(
            Range.closed(
              new RowKeyWrapper(BytesConverter.toBytes(0L: Long)),
              new RowKeyWrapper(BytesConverter.toBytes(Long.MaxValue))
            )
          )
        }
        ImmutableRangeSet.copyOf(rangeSetResult)
      case typedVal: Array[Byte] =>
        ImmutableRangeSet.of(
          Range.atLeast(
            new RowKeyWrapper(typedVal)
          )
        )
      case typedVal: String =>
        val valueBytes: Array[Byte] = BytesConverter.toBytes(typedVal)
        ImmutableRangeSet.of(
          Range.atLeast(
            new RowKeyWrapper(valueBytes)
          )
        )
      case typedVal: UTF8String =>
        ImmutableRangeSet.of(
          Range.atLeast(
            new RowKeyWrapper(typedVal.getBytes)
          )
        )
      case _ => ImmutableRangeSet.of(Range.all[RowKeyWrapper]())
    }
}
