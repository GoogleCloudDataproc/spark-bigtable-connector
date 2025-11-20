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

import com.google.cloud.bigtable.data.v2.models.Filters.{FILTERS, Filter}
import com.google.bigtable.v2.RowFilter
import com.google.common.io.BaseEncoding

object RowFilterUtils {

  def encode(filter: Filter): String = {
    BaseEncoding.base64().encode(filter.toProto.toByteArray)
  }

  def decode(protoString: String): Filter = {
    val decodedBytes = BaseEncoding.base64().decode(protoString)
    val deserializedProto = RowFilter.parseFrom(decodedBytes)
    FILTERS.fromProto(deserializedProto)
  }

}
