/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigtable

import com.google.cloud.spark.bigtable.datasources.{BytesConverter, Utils}
import org.scalatest.funsuite.AnyFunSuite

class StartsWithTest extends AnyFunSuite with Logging {
  test("simple1") {
    val t = new Array[Byte](2)
    t(0) = 1.toByte
    t(1) = 2.toByte
    val expected = new Array[Byte](2)
    expected(0) = 1.toByte
    expected(1) = 3.toByte
    val res = Utils.incrementByteArray(t)
    assert(res.sameElements(expected))
  }

  test("simple2") {
    val t = new Array[Byte](1)
    t(0) = 87.toByte
    val expected = new Array[Byte](1)
    expected(0) = 88.toByte
    val res = Utils.incrementByteArray(t)
    assert(res.sameElements(expected))
  }

  test("overflow1") {
    val t = new Array[Byte](2)
    t(0) = 1.toByte
    t(1) = (-1).toByte
    val expected = new Array[Byte](1)
    expected(0) = 2.toByte
    val res = Utils.incrementByteArray(t)
    assert(res.sameElements(expected))
  }

  test("overflow2") {
    val t = new Array[Byte](2)
    t(0) = (-1).toByte
    t(1) = (-1).toByte
    val expected = null
    val res = Utils.incrementByteArray(t)
    assert(res == expected)
  }

  test("overflow3_multiple") {
    val t = new Array[Byte](4)
    t(0) = (-1).toByte
    t(1) = 10.toByte
    t(2) = (-1).toByte
    t(3) = (-1).toByte
    val expected = new Array[Byte](2)
    expected(0) = (-1).toByte
    expected(1) = 11.toByte
    val res = Utils.incrementByteArray(t)
    assert(res.sameElements(expected))
  }

  test("max-min-value") {
    val t = new Array[Byte](2)
    t(0) = 1.toByte
    t(1) = (127).toByte
    val expected = new Array[Byte](2)
    expected(0) = 1.toByte
    expected(1) = (-128).toByte
    val res = Utils.incrementByteArray(t)
    assert(res.sameElements(expected))
  }

  test("complicated") {
    val input = "row005"
    val expectedOutput = "row006"
    val t = BytesConverter.toBytes(input)
    val expected = BytesConverter.toBytes(expectedOutput)
    val res = Utils.incrementByteArray(t)
    assert(res.sameElements(expected))
  }
}
