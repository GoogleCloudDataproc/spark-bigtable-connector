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

package com.google.cloud.spark.bigtable.datasources

import org.apache.spark.sql.types._
import org.apache.yetus.audience.InterfaceAudience

import java.sql.{Date, Timestamp}

@InterfaceAudience.Private
object Utils {
  // increment Byte array's value by 1
  def incrementByteArray(array: Array[Byte]): Array[Byte] = {
    if (array.length == 0) {
      return null
    }
    var index = -1 // index of the byte we have to increment
    var a = array.length - 1

    while (a >= 0) {
      if (array(a) != (-1).toByte) {
        index = a
        a = -1 // break from the loop because we found a non -1 element
      }
      a = a - 1
    }

    if (index < 0) {
      return null
    }
    val returnArray = new Array[Byte](index + 1)

    for (a <- 0 until index) {
      returnArray(a) = array(a)
    }
    returnArray(index) = (array(index) + 1).toByte

    returnArray
  }
}
