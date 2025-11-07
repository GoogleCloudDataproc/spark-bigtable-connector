/*
 * Copyright 2025 Google LLC
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

package com.google.cloud.spark.bigtable.util

import com.google.cloud.bigtable.data.v2.models.Filters.FILTERS
import com.google.cloud.spark.bigtable.Logging
import org.scalatest.funsuite.AnyFunSuite

class RowFilterUtilsTest
  extends AnyFunSuite
    with Logging {
  test("encode_decode") {

    val filter = FILTERS.chain()
      .filter(FILTERS.qualifier().regex("abc.*"))
      .filter(FILTERS.value().range().startClosed("a").endOpen("b"))

    val filterString = RowFilterUtils.encode(filter)

    val value = RowFilterUtils.decode(filterString)

    assert(value.toProto == filter.toProto)
  }
}
