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

import com.google.cloud.spark.bigtable.datasources._
import com.google.common.collect.RangeSet
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._

class StringStartsWithFilterAdapterTest extends AnyFunSuite {
  test("adaptStringAndUtf8Value") {
    val testData = Table(
      ("value", "valuesInRangeSet", "valuesOutsideRangeSet"),
      ("", Array[String]("", "foo", "\u0000"), Array[String]()),
      (
        "foo",
        Array[String]("foo", "foo\u0000", "foooo"),
        Array[String]("", "fo", "fon", "fop", "\u0002", null)
      )
    )
    forAll(testData) {
      (
          value: String,
          valuesInRangeSet: Array[String],
          valuesOutsideRangeSet: Array[String]
      ) =>
        val resultRangeSet: RangeSet[RowKeyWrapper] =
          StringStartsWithFilterAdapter.convertValueToRangeSet(value)
        valuesInRangeSet.foreach(v =>
          assert(
            resultRangeSet.contains(
              new RowKeyWrapper(
                if (v == null) null else BytesConverter.toBytes(v)
              )
            )
          )
        )
        valuesOutsideRangeSet.foreach(v =>
          assert(
            !resultRangeSet.contains(
              new RowKeyWrapper(
                if (v == null) null else BytesConverter.toBytes(v)
              )
            )
          )
        )

        val utf8ResultRangeSet: RangeSet[RowKeyWrapper] =
          StringStartsWithFilterAdapter.convertValueToRangeSet(
            UTF8String.fromString(value)
          )
        valuesInRangeSet.foreach(v =>
          assert(
            utf8ResultRangeSet.contains(
              new RowKeyWrapper(
                if (v == null) null else UTF8String.fromString(v).getBytes
              )
            )
          )
        )
        valuesOutsideRangeSet.foreach(v =>
          assert(
            !utf8ResultRangeSet.contains(
              new RowKeyWrapper(
                if (v == null) null else UTF8String.fromString(v).getBytes
              )
            )
          )
        )
    }
  }
}
