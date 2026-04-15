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

package com.google.cloud.spark.bigtable.model;

import java.io.Serializable;
import java.util.Arrays;

/** A JavaBean class for using as a DataFrame row in tests with specified size. */
public class TestSizedRow implements Serializable {
  private String stringCol;
  private byte[] bytesCol;

  public TestSizedRow() {}

  public TestSizedRow(String stringCol, byte[] bytesCol) {
    this.stringCol = stringCol;
    this.bytesCol = bytesCol;
  }

  public String getStringCol() {
    return stringCol;
  }

  public void setStringCol(String stringCol) {
    this.stringCol = stringCol;
  }

  public byte[] getBytesCol() {
    return bytesCol;
  }

  public void setBytesCol(byte[] bytesCol) {
    this.bytesCol = bytesCol;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((stringCol == null) ? 0 : stringCol.hashCode());
    result = prime * result + Arrays.hashCode(bytesCol);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    TestSizedRow other = (TestSizedRow) obj;
    if (stringCol == null) {
      if (other.stringCol != null) return false;
    } else if (!stringCol.equals(other.stringCol)) return false;
    if (!Arrays.equals(bytesCol, other.bytesCol)) return false;
    return true;
  }
}
