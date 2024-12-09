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

package com.google.cloud.spark.bigtable

import com.google.cloud.spark.bigtable.datasources._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks._

class BytesConverterTest extends AnyFunSuite {
  test("booleanToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      (true, Array[Byte](-1)),
      (false, Array[Byte](0))
    )
    forAll(testData) { (input: Boolean, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("shortToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      (0.toShort, Array[Byte](0, 0)),
      (7249.toShort, Array[Byte](28, 81)),
      (-4351.toShort, Array[Byte](-17, 1))
    )
    forAll(testData) { (input: Short, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("intToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      (0, Array[Byte](0, 0, 0, 0)),
      (95920035, Array[Byte](5, -73, -97, -93)),
      (-3520532, Array[Byte](-1, -54, 71, -20))
    )
    forAll(testData) { (input: Int, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("longToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      (0L, Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)),
      (10324949000235523L, Array[Byte](0, 36, -82, 124, -123, 123, -86, 3)),
      (-1300130019358424L, Array[Byte](-1, -5, 97, -119, -28, 8, -55, 40))
    )
    forAll(testData) { (input: Long, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("floatToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      (0.0f, Array[Byte](0, 0, 0, 0)),
      (3473.359f, Array[Byte](69, 89, 21, -66)),
      (-7048.991f, Array[Byte](-59, -36, 71, -18))
    )
    forAll(testData) { (input: Float, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("doubleToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      (0.0, Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)),
      (46240104.34664, Array[Byte](65, -122, 12, -117, 66, -59, -21, 49)),
      (-245.1405099694, Array[Byte](-64, 110, -92, 127, 14, -61, 106, -72))
    )
    forAll(testData) { (input: Double, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("stringToBytes") {
    val testData = Table(
      ("input", "expectedBytes"),
      ("", Array[Byte]()),
      ("foobar9023", Array[Byte](102, 111, 111, 98, 97, 114, 57, 48, 50, 51)),
      ("BARFOO^@)*[", Array[Byte](66, 65, 82, 70, 79, 79, 94, 64, 41, 42, 91)),
      ("BARFOO^@)*[", Array[Byte](66, 65, 82, 70, 79, 79, 94, 64, 41, 42, 91)),
      ("\n\u0000\t\u0001", Array[Byte](10, 0, 9, 1)),
      (
        "\u0493\u03ce\u2394\u01e9\u4ee2\ua312",
        Array[Byte](-46, -109, -49, -114, -30, -114, -108, -57, -87, -28, -69,
          -94, -22, -116, -110)
      )
    )
    forAll(testData) { (input: String, expectedBytes: Array[Byte]) =>
      val result = BytesConverter.toBytes(input)
      assert(result.sameElements(expectedBytes))
    }
  }

  test("bytesToBoolean") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (Array[Byte](0), 0, false),
      (Array[Byte](-1, 0), 0, true),
      (Array[Byte](-1, 0, -1), 1, false),
      (Array[Byte](0, 0, -1), 2, true)
    )
    forAll(testData) {
      (inputBytes: Array[Byte], offset: Int, expected: Boolean) =>
        val result = BytesConverter.toBoolean(inputBytes, offset)
        assert(result == expected)
    }
  }

  test("bytesToShort") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (Array[Byte](0, 0), 0, 0.toShort),
      (Array[Byte](12, 43), 0, 3115.toShort),
      (Array[Byte](-12, 33, -53), 0, -3039.toShort),
      (Array[Byte](-12, 99, -21), 1, 25579.toShort),
      (Array[Byte](-12, 99, -21, 32, -10), 2, -5344.toShort)
    )
    forAll(testData) {
      (inputBytes: Array[Byte], offset: Int, expected: Short) =>
        val result = BytesConverter.toShort(inputBytes, offset)
        assert(result == expected)
    }
  }

  test("bytesToInt") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (Array[Byte](0, 0, 0, 0), 0, 0),
      (Array[Byte](4, 5, 6, 7), 0, 67438087),
      (Array[Byte](4, -50, 6, -70, 10), 1, -838419958),
      (Array[Byte](4, -50, 0, -70, 10, 80, 90), 2, 12192336)
    )
    forAll(testData) { (inputBytes: Array[Byte], offset: Int, expected: Int) =>
      val result = BytesConverter.toInt(inputBytes, offset)
      assert(result == expected)
    }
  }

  test("bytesToLong") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (Array[Byte](0, 0, 0, 0, 0, 0, 0, 0), 0, 0L),
      (Array[Byte](0, 0, 0, 0, 0, 2, 0, 1), 0, 131073L),
      (Array[Byte](0, 1, 4, 5, 0, 2, 4, 0), 0, 285894498190336L),
      (Array[Byte](10, -10, 20, 3, 5, 15, 60, 7, 0), 1, -714943120579754240L),
      (Array[Byte](0, 1, 2, 3, 5, 9, 12, 16, 21, 12), 1, 72623864152854549L)
    )
    forAll(testData) { (inputBytes: Array[Byte], offset: Int, expected: Long) =>
      val result = BytesConverter.toLong(inputBytes, offset)
      assert(result == expected)
    }
  }

  test("bytesToFloat") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (Array[Byte](0, 0, 0, 0), 0, 0.0f),
      (Array[Byte](68, 89, 22, -66), 0, 868.35535f),
      (Array[Byte](10, 2, 14, 43, 12), 1, 1.0444866e-37f),
      (Array[Byte](-10, -20, 30, 4, 2, -19), 1, -7.641168e26f)
    )
    forAll(testData) {
      (inputBytes: Array[Byte], offset: Int, expected: Float) =>
        val result = BytesConverter.toFloat(inputBytes, offset)
        assert(result == expected)
    }
  }

  test("bytesToDouble") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (Array[Byte](0, 0, 0, 0, 0, 0, 0, 0), 0, 0.0),
      (Array[Byte](63, -12, 12, -117, 60, -59, -21, 49), 0, 1.2530624746274948),
      (
        Array[Byte](10, -63, -112, 1, -17, 60, -59, -21, 4),
        1,
        -6.714055919327933e7
      ),
      (Array[Byte](0, 100, 5, 0, 1, 0, 2, 11, 10, 0), 1, 6.492427756269094e173),
      (
        Array[Byte](0, -15, 5, 2, 1, 89, 2, 11, 10, 0),
        1,
        -2.6718241515122167e236
      )
    )
    forAll(testData) {
      (inputBytes: Array[Byte], offset: Int, expected: Double) =>
        val result = BytesConverter.toDouble(inputBytes, offset)
        assert(result == expected)
    }
  }

  test("bytesToString") {
    val testData = Table(
      ("inputBytes", "offset", "expected"),
      (null, 0, null),
      (Array[Byte](), 0, ""),
      (Array[Byte](100, 110), 2, ""),
      (Array[Byte](115, 110, 100, 90, 85, 60, 35), 0, "sndZU<#"),
      (Array[Byte](15, 10, 0, 9, 1), 1, "\n\u0000\t\u0001"),
      (
        Array[Byte](10, -114, -30, -114, -108, -57, -87, -28, -69, -94),
        2,
        "\u2394\u01e9\u4ee2"
      )
    )
    forAll(testData) {
      (inputBytes: Array[Byte], offset: Int, expected: String) =>
        val result = BytesConverter.toString(inputBytes, offset)
        assert(result == expected)
    }
  }

  // Since these two functions are logically identical, we use a single test.
  test("simpleCompareToAndEquals") {
    val testData = Table(
      ("left", "right", "expectedCompareTo"),
      (Array[Byte](1, 2, 3), Array[Byte](1, 1, 8), 1),
      (Array[Byte](1, 2, 3, 4), Array[Byte](1, 2, 3, 4, 5), -1),
      (Array[Byte](1, 2, 3, 4), Array[Byte](1, 2, 3, 4), 0),
      (Array[Byte](1, 2, 4, 1), Array[Byte](1, 2, 3, 9, 9), 1),
      (Array[Byte](-2, 9, 9, 1), Array[Byte](-1, 2, 3, 9, 9), -1),
      (Array[Byte](), Array[Byte](1), -1),
      (Array[Byte](), Array[Byte](), 0),
      (null, null, 0),
      (null, Array[Byte](), 0)
    )
    forAll(testData) {
      (left: Array[Byte], right: Array[Byte], expectedCompareTo: Int) =>
        val compareToResult = BytesConverter.compareTo(left, right)
        expectedCompareTo match {
          case expected if expected > 0 => assert(compareToResult > 0)
          case expected if expected < 0 => assert(compareToResult < 0)
          case _                        => assert(compareToResult == 0)
        }
        val equalsResult = BytesConverter.equals(left, right)
        assert(equalsResult == (expectedCompareTo == 0))
    }
  }

  // Since these two functions are logically identical, we use a single test.
  test("compareToAndEquals") {
    val testData = Table(
      (
        "left",
        "leftOffset",
        "leftLen",
        "right",
        "rightOffset",
        "rightLen",
        "throwsException",
        "expectedCompareTo"
      ),
      (Array[Byte](1, 2, 3), 0, 3, Array[Byte](1, 1, 8), 0, 3, false, 1),
      (Array[Byte](-2, 2, 3), 0, 3, Array[Byte](-1, 1, 8), 0, 3, false, -1),
      (
        Array[Byte](1, 2, 3, 4),
        0,
        4,
        Array[Byte](1, 2, 3, 4, 5),
        0,
        5,
        false,
        -1
      ),
      (Array[Byte](1, 2, 3, 4), 1, 3, Array[Byte](1, 2, 3, 4), 0, 4, false, 1),
      (Array[Byte](1, 2, 3, 4), 4, 0, Array[Byte](1), 0, 1, false, -1),
      (Array[Byte](1), 0, 1, Array[Byte](), 0, 0, false, 1),
      (Array[Byte](), 0, 0, Array[Byte](), 3, 0, false, 0),
      (Array[Byte](1, 2), 2, 0, Array[Byte](3, 4), 2, 0, false, 0),
      (
        Array[Byte](1, 2, 3, 4, 5),
        1,
        3,
        Array[Byte](2, 3, 4, 6),
        0,
        3,
        false,
        0
      ),
      (Array[Byte](1), 0, 1, Array[Byte](), 0, 1, true, 1),
      (Array[Byte](1, 2, 3, 4), 0, 5, Array[Byte](1, 2, 3, 4, 5), 0, 5, true, 0)
    )
    forAll(testData) {
      (
          left: Array[Byte],
          leftOffset: Int,
          leftLen: Int,
          right: Array[Byte],
          rightOffset: Int,
          rightLen: Int,
          throwsException: Boolean,
          expectedCompareTo: Int
      ) =>
        try {
          val compareToResult = BytesConverter.compareTo(
            left,
            leftOffset,
            leftLen,
            right,
            rightOffset,
            rightLen
          )
          if (throwsException) {
            assert(
              false,
              "compareTo Should have thrown an ArrayIndexOutOfBoundsException exception."
            )
          }
          expectedCompareTo match {
            case expected if expected > 0 => assert(compareToResult > 0)
            case expected if expected < 0 => assert(compareToResult < 0)
            case _                        => assert(compareToResult == 0)
          }
          val equalsResult = BytesConverter.equals(
            left,
            leftOffset,
            leftLen,
            right,
            rightOffset,
            rightLen
          )
          if (throwsException) {
            assert(
              false,
              "equals Should have thrown an ArrayIndexOutOfBoundsException exception."
            )
          }
          assert(equalsResult == (expectedCompareTo == 0))
        } catch {
          case e: ArrayIndexOutOfBoundsException => {
            if (!throwsException) {
              assert(
                false,
                "Should not have thrown an ArrayIndexOutOfBoundsException exeption."
              )
            }
          }
        }
    }
  }
}

class DataTypeBytesTest extends AnyFunSuite {
  // We want to make sure we get notified through test breakage if the values
  // of these ever change, to ensure backward compatibility.
  test("dataTypeBytesValues") {
    assert(DataTypeBytes.BYTE_SIZE == 8)
    assert(DataTypeBytes.BYTE_BYTES == 1)
    assert(DataTypeBytes.BOOLEAN_BYTES == 1)
    assert(DataTypeBytes.SHORT_BYTES == 2)
    assert(DataTypeBytes.INT_BYTES == 4)
    assert(DataTypeBytes.LONG_BYTES == 8)
    assert(DataTypeBytes.FLOAT_BYTES == 4)
    assert(DataTypeBytes.DOUBLE_BYTES == 8)
  }
}
