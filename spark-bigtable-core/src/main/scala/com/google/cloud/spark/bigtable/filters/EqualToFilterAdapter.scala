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
import com.google.common.collect.{ImmutableRangeSet, Range, RangeSet}
import org.apache.spark.unsafe.types.UTF8String

object EqualToFilterAdapter {
  def convertValueToRangeSet(value: Any): RangeSet[RowKeyWrapper] =
    value match {
      case typedVal: Long =>
        ImmutableRangeSet.of(
          Range.singleton(new RowKeyWrapper(BytesConverter.toBytes(typedVal)))
        )
      case typedVal: Array[Byte] =>
        ImmutableRangeSet.of(Range.singleton(new RowKeyWrapper(typedVal)))
      case typedVal: String =>
        ImmutableRangeSet.of(
          Range.singleton(new RowKeyWrapper(BytesConverter.toBytes(typedVal)))
        )
      case typedVal: UTF8String =>
        ImmutableRangeSet.of(
          Range.singleton(new RowKeyWrapper(typedVal.getBytes))
        )
      case _ => ImmutableRangeSet.of(Range.all[RowKeyWrapper]())
    }
}
