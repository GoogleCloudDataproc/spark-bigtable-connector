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

import org.apache.yetus.audience.InterfaceAudience

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

// Helper class to convert data types to bytes and back. Since Spark uses Long,
//  Float, etc, but Bigtable treats all data as byte strings for most purposes.
@InterfaceAudience.Private
object BytesConverter {
  // True becomes -1 and false 0.
  def toBytes(value: Boolean): Array[Byte] = {
    if (value) Array[Byte]((-1).toByte) else Array[Byte](0.toByte)
  }

  def toBytes(value: Short): Array[Byte] = {
    ByteBuffer.allocate(DataTypeBytes.SHORT_BYTES).putShort(value).array()
  }

  // Scala's native Integer type is `Int` (`Integer` exists to maintain
  // compatibility with Java).
  def toBytes(value: Int): Array[Byte] = {
    ByteBuffer.allocate(DataTypeBytes.INT_BYTES).putInt(value).array()
  }

  def toBytes(value: Long): Array[Byte] = {
    ByteBuffer.allocate(DataTypeBytes.LONG_BYTES).putLong(value).array()
  }

  def toBytes(value: Float): Array[Byte] = {
    ByteBuffer.allocate(DataTypeBytes.FLOAT_BYTES).putFloat(value).array()
  }

  def toBytes(value: Double): Array[Byte] = {
    ByteBuffer.allocate(DataTypeBytes.DOUBLE_BYTES).putDouble(value).array()
  }

  // Since java.nio.ByteBuffer does not support strings, we convert manually.
  def toBytes(value: String): Array[Byte] = {
    value.getBytes(StandardCharsets.UTF_8)
  }

  def toBoolean(arr: Array[Byte], offset: Int = 0): Boolean = {
    arr(offset) != 0
  }

  def toShort(arr: Array[Byte], offset: Int = 0): Short = {
    ByteBuffer.wrap(arr).getShort(offset)
  }

  def toInt(arr: Array[Byte], offset: Int = 0): Int = {
    ByteBuffer.wrap(arr).getInt(offset)
  }

  def toLong(arr: Array[Byte], offset: Int = 0): Long = {
    ByteBuffer.wrap(arr).getLong(offset)
  }

  def toFloat(arr: Array[Byte], offset: Int = 0): Float = {
    ByteBuffer.wrap(arr).getFloat(offset)
  }

  def toDouble(arr: Array[Byte], offset: Int = 0): Double = {
    ByteBuffer.wrap(arr).getDouble(offset)
  }

  // Since java.nio.ByteBuffer does not support string, we convert manually.
  def toString(arr: Array[Byte], offset: Int = 0): String = {
    if (arr == null) {
      null
    } else if (arr.length - offset <= 0) {
      ""
    } else {
      new String(arr.slice(offset, arr.length), StandardCharsets.UTF_8)
    }
  }

  def compareTo(left: Array[Byte], right: Array[Byte]): Int = {
    compareTo(
      left,
      0,
      if (left == null) 0 else left.length,
      right,
      0,
      if (right == null) 0 else right.length
    )
  }

  /** Compares two Byte arrays and returns the result. For each array, the range
    * [offset, offset + length) is considered and an exception is thrown if this
    * specified range exceeds the array length.
    */
  def compareTo(
      left: Array[Byte],
      leftOffset: Int,
      leftLen: Int,
      right: Array[Byte],
      rightOffset: Int,
      rightLen: Int
  ): Int = {
    if (left == right && leftOffset == rightOffset && leftLen == rightLen) {
      0
    } else {
      val end1: Int = leftOffset + leftLen
      val end2: Int = rightOffset + rightLen
      var i: Int = leftOffset
      var j: Int = rightOffset
      while (i < end1 && j < end2) {
        val a: Int = (left(i) & 0xff)
        val b: Int = (right(j) & 0xff)
        if (a != b) {
          return a - b
        }
        i += 1
        j += 1
      }
      return (leftLen - rightLen)
    }
  }

  def equals(left: Array[Byte], right: Array[Byte]): Boolean = {
    return equals(
      left,
      0,
      if (left == null) 0 else left.length,
      right,
      0,
      if (right == null) 0 else right.length
    )
  }

  /** Returns whether the subarrays of two arrays are equal. For each array, the
    * range [offset, offset + length) is considered and an exception is thrown
    * if this specified range exceeds the array length.
    */
  def equals(
      left: Array[Byte],
      leftOffset: Int,
      leftLen: Int,
      right: Array[Byte],
      rightOffset: Int,
      rightLen: Int
  ): Boolean = {
    return (compareTo(
      left,
      leftOffset,
      leftLen,
      right,
      rightOffset,
      rightLen
    ) == 0)
  }
}

@InterfaceAudience.Private
object DataTypeBytes {
  // SIZE indicate the number of bits.
  val BYTE_SIZE = java.lang.Byte.SIZE
  // We use the same calculations as in the HBase Bytes class to maintain
  //  consistency with the original implementation.
  val BYTE_BYTES = BYTE_SIZE / BYTE_SIZE
  val BOOLEAN_BYTES = BYTE_SIZE / BYTE_SIZE
  val SHORT_BYTES = java.lang.Short.SIZE / BYTE_SIZE
  val INT_BYTES = java.lang.Integer.SIZE / BYTE_SIZE
  val LONG_BYTES = java.lang.Long.SIZE / BYTE_SIZE
  val FLOAT_BYTES = java.lang.Float.SIZE / BYTE_SIZE
  val DOUBLE_BYTES = java.lang.Double.SIZE / BYTE_SIZE
}
