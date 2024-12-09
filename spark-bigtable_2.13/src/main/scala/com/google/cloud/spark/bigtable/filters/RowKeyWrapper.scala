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

import com.google.cloud.bigtable.data.v2.internal.ByteStringComparator
import com.google.protobuf.ByteString

class RowKeyWrapper(private val key: ByteString)
    extends Comparable[RowKeyWrapper]
    with Serializable {
  def this(byteArrayKey: Array[Byte]) {
    this(
      if (byteArrayKey == null)
        null
      else
        ByteString.copyFrom(byteArrayKey)
    )
  }

  def getKey: ByteString = key

  override def compareTo(o: RowKeyWrapper): Int = compare(key, o.key)

  private def compare(key1: ByteString, key2: ByteString): Int = {
    if (key1 == null) {
      if (key2 == null) {
        return 0
      } else {
        return 1
      }
    } else if (key2 == null) {
      return -1
    }
    ByteStringComparator.INSTANCE.compare(key1, key2)
  }

  override def toString: String = if (key == null) "<null>" else key.toString
}
