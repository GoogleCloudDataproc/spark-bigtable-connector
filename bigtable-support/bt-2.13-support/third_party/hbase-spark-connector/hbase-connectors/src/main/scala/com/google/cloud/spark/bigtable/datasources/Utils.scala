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

  /** Parses the Bigtable field to its corresponding
    * Scala type which can then be put into a Spark GenericRow
    * which is then automatically converted by Spark.
    */
  def bigtableFieldToScalaType(
      f: Field,
      src: Array[Byte],
      offset: Int,
      length: Int
  ): Any = {
    if (f.exeSchema.isDefined) {
      // If we have avro schema defined, use it to get record, and then convert them to catalyst data type
      val m = AvroSerdes.deserialize(src, f.exeSchema.get)
      val n = f.avroToCatalyst.map(_(m))
      n.get
    } else {
      // Fall back to atomic type
      f.dt match {
        case BooleanType   => BytesConverter.toBoolean(src, offset)
        case ByteType      => src(offset)
        case ShortType     => BytesConverter.toShort(src, offset)
        case IntegerType   => BytesConverter.toInt(src, offset)
        case LongType      => BytesConverter.toLong(src, offset)
        case FloatType     => BytesConverter.toFloat(src, offset)
        case DoubleType    => BytesConverter.toDouble(src, offset)
        case DateType      => new Date(BytesConverter.toLong(src, offset))
        case TimestampType => new Timestamp(BytesConverter.toLong(src, offset))
        case StringType =>
          BytesConverter.toString(src.slice(offset, offset + length))
        case BinaryType =>
          val newArray = new Array[Byte](length)
          System.arraycopy(src, offset, newArray, 0, length)
          newArray
        // TODO: SparkSqlSerializer.deserialize[Any](src)
        case _ => throw new Exception(s"unsupported data type ${f.dt}")
      }
    }
  }

  // convert input to data type
  def toBytes(input: Any, field: Field): Array[Byte] = {
    if (field.schema.isDefined) {
      // Here we assume the top level type is structType
      val record = field.catalystToAvro(input)
      AvroSerdes.serialize(record, field.schema.get)
    } else {
      field.dt match {
        case BooleanType => BytesConverter.toBytes(input.asInstanceOf[Boolean])
        case ByteType    => Array(input.asInstanceOf[Number].byteValue)
        case ShortType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].shortValue)
        case IntegerType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].intValue)
        case LongType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].longValue)
        case FloatType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].floatValue)
        case DoubleType =>
          BytesConverter.toBytes(input.asInstanceOf[Number].doubleValue)
        case DateType | TimestampType =>
          BytesConverter.toBytes(input.asInstanceOf[java.util.Date].getTime)
        case StringType => BytesConverter.toBytes(input.toString)
        case BinaryType => input.asInstanceOf[Array[Byte]]
        case _          => throw new Exception(s"unsupported data type ${field.dt}")
      }
    }
  }

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
