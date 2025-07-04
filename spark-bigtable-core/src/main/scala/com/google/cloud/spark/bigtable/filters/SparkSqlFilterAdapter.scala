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

import com.google.cloud.spark.bigtable.datasources.BigtableTableCatalog
import com.google.common.collect.{ImmutableRangeSet, Range, RangeSet, TreeRangeSet}
import org.apache.spark.sql.sources.{
  And,
  EqualTo,
  Filter,
  GreaterThan,
  GreaterThanOrEqual,
  In,
  LessThan,
  LessThanOrEqual,
  Or,
  StringStartsWith
}

/** This class converts Spark SQL filters to row key ranges which
  * will be pushed down to Bigtable.
  *
  * We don't push down compound row key filters to Bigtable since they
  * can result in incorrect results. For example, if row key is the
  * concatenation of three Spark columns A:B:C and the Spark filter is
  * B=b, we cannot convert this into a row key read range in Bigtable.
  */
object SparkSqlFilterAdapter {

  def createRowKeyRangeSet(
      filters: Array[Filter],
      catalog: BigtableTableCatalog,
      pushDownRowKeyFilters: Boolean
  ): RangeSet[RowKeyWrapper] = {
    if (pushDownRowKeyFilters) {
      val rangeSetResult: RangeSet[RowKeyWrapper] =
        TreeRangeSet.create[RowKeyWrapper]()
      rangeSetResult.add(Range.all[RowKeyWrapper]())
      filters.foreach(x =>
        rangeSetResult.removeAll(
          catalog.row.convertFilterToRangeSet(x).complement()
        )
      )
      ImmutableRangeSet.copyOf(rangeSetResult)
    } else {
      ImmutableRangeSet.of(Range.all[RowKeyWrapper]())
    }
  }
}
