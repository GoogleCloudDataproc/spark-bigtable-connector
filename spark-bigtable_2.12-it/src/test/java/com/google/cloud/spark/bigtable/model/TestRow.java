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

/**
 * A JavaBean class for using as a DataFrame row in tests.
 */
public class TestRow implements Serializable {
  private String stringCol;
  private String stringCol2;
  private long longCol;

  public TestRow() {
  }

  public TestRow(String stringCol, String stringCol2, long longCol) {
    this.stringCol = stringCol;
    this.stringCol2 = stringCol2;
    this.longCol = longCol;
  }

  public String getStringCol() {
    return stringCol;
  }

  public void setStringCol(String stringCol) {
    this.stringCol = stringCol;
  }

  public String getStringCol2() {
    return stringCol2;
  }

  public void setStringCol2(String stringCol2) {
    this.stringCol2 = stringCol2;
  }

  public long getLongCol() {
    return longCol;
  }

  public void setLongCol(long longCol) {
    this.longCol = longCol;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((stringCol == null) ? 0 : stringCol.hashCode());
    result = prime * result + ((stringCol2 == null) ? 0 : stringCol2.hashCode());
    result = prime * result + (int) (longCol ^ (longCol >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    TestRow other = (TestRow) obj;
    if (stringCol == null) {
      if (other.stringCol != null) return false;
    } else if (!stringCol.equals(other.stringCol)) return false;
    if (stringCol2 == null) {
      if (other.stringCol2 != null) return false;
    } else if (!stringCol2.equals(other.stringCol2)) return false;
    if (longCol != other.longCol) return false;
    return true;
  }

  @Override
  public String toString() {
    return "TestRow [stringCol="
        + stringCol
        + ", stringCol2="
        + stringCol2
        + ", longCol="
        + longCol
        + "]";
  }
}
