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

class GreaterThanFilterAdapterTest extends AnyFunSuite {
  test("adaptLongValue") {
    val testData = Table(
      ("value", "valuesInRangeSet", "valuesOutsideRangeSet"),
      (
        Long.MaxValue,
        Array[Long](),
        Array[Long](Long.MaxValue, 10L, 0L, -1L, Long.MinValue)
      ),
      (
        10L,
        Array[Long](11L, Long.MaxValue),
        Array[Long](10L, 0L, -1L, Long.MinValue)
      ),
      (0L, Array[Long](3L, Long.MaxValue), Array[Long](0L, -1L, Long.MinValue)),
      (
        -1L,
        Array[Long](0L, 10, Long.MaxValue),
        Array[Long](-1L, -25L, Long.MinValue)
      ),
      (
        -150L,
        Array[Long](-1L, 0L, 10, Long.MaxValue),
        Array[Long](-150L, -252L, Long.MinValue)
      ),
      (
        Long.MinValue,
        Array[Long](-100L, -1L, 0L, 10, Long.MaxValue),
        Array[Long](Long.MinValue)
      )
    )
    forAll(testData) {
      (
          value: Long,
          valuesInRangeSet: Array[Long],
          valuesOutsideRangeSet: Array[Long]
      ) =>
        val resultRangeSet: RangeSet[RowKeyWrapper] =
          GreaterThanFilterAdapter.convertValueToRangeSet(value)
        valuesInRangeSet.foreach(v =>
          assert(
            resultRangeSet.contains(
              new RowKeyWrapper(BytesConverter.toBytes(v))
            )
          )
        )
        valuesOutsideRangeSet.foreach(v =>
          assert(
            !resultRangeSet.contains(
              new RowKeyWrapper(BytesConverter.toBytes(v))
            )
          )
        )
    }
  }

  test("adaptByteArrayValue") {
    val testData = Table(
      // Negative numbers have a *larger* byte encoding
      ("value", "valuesInRangeSet", "valuesOutsideRangeSet"),
      // Simple array with one element
      (
        Array[Byte](10),
        Array[Array[Byte]](Array[Byte](10, 15), Array[Byte](-3), null),
        Array[Array[Byte]](
          Array[Byte](),
          Array[Byte](3, 5),
          Array[Byte](10)
        )
      ),
      // Empty array should be smaller than everything
      (
        Array[Byte](),
        Array[Array[Byte]](Array[Byte](10), null),
        Array[Array[Byte]](
          Array[Byte]()
        )
      ),
      // Array with multiple elements
      (
        Array[Byte](-5, 0, 10),
        Array[Array[Byte]](
          Array[Byte](-5, 1),
          null
        ),
        Array[Array[Byte]](
          Array[Byte](1, 10),
          Array[Byte](-5, 0),
          Array[Byte](-5, 0, 9),
          Array[Byte](-5, 0, 10)
        )
      )
    )
    forAll(testData) {
      (
          value: Array[Byte],
          valuesInRangeSet: Array[Array[Byte]],
          valuesOutsideRangeSet: Array[Array[Byte]]
      ) =>
        val resultRangeSet: RangeSet[RowKeyWrapper] =
          GreaterThanFilterAdapter.convertValueToRangeSet(value)
        valuesInRangeSet.foreach(v =>
          assert(resultRangeSet.contains(new RowKeyWrapper(v)))
        )
        valuesOutsideRangeSet.foreach(v =>
          assert(!resultRangeSet.contains(new RowKeyWrapper(v)))
        )
    }
  }

  test("adaptStringAndUtf8Value") {
    val testData = Table(
      ("value", "valuesInRangeSet", "valuesOutsideRangeSet"),
      ("", Array[String]("foo", "\u0000"), Array[String]("")),
      (
        "foo",
        Array[String]("zoooo", null),
        Array[String]("", "fo", "foo", "\u0002")
      )
    )
    forAll(testData) {
      (
          value: String,
          valuesInRangeSet: Array[String],
          valuesOutsideRangeSet: Array[String]
      ) =>
        val resultRangeSet: RangeSet[RowKeyWrapper] =
          GreaterThanFilterAdapter.convertValueToRangeSet(value)
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
          GreaterThanFilterAdapter.convertValueToRangeSet(
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
