package com.google.cloud.spark.bigtable.filters

import com.google.common.collect.{ImmutableRangeSet, RangeSet, TreeRangeSet}

object IsInFilterAdapter {
  def convertValuesToRangeSet(values: Array[Any]): RangeSet[RowKeyWrapper] = {
    val rangeSet = TreeRangeSet.create[RowKeyWrapper]()
    values.foreach { value =>
      rangeSet.addAll(EqualToFilterAdapter.convertValueToRangeSet(value))
    }
    ImmutableRangeSet.copyOf(rangeSet)
  }
}
