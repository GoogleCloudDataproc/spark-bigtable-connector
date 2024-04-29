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
import java.util.Objects;

public class Favorites implements Serializable {
  private String favorite_string;
  private int favorite_number;

  public Favorites() {
  }

  public Favorites(String string, int number) {
    this.favorite_string = string;
    this.favorite_number = number;
  }

  public String getFavoriteString() {
    return favorite_string;
  }

  public void setFavoriteString(String string) {
    this.favorite_string = string;
  }

  public int getFavoriteNumber() {
    return favorite_number;
  }

  public void setFavoriteNumber(int number) {
    this.favorite_number = number;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Favorites)) {
      return false;
    }
    Favorites fav = (Favorites) o;
    return Objects.equals(favorite_string, fav.favorite_string)
        && Objects.equals((Integer) favorite_number, (Integer) fav.favorite_number);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this);
  }

  @Override
  public String toString() {
    return "Favorites{"
        + "favorite_string='"
        + favorite_string
        + '\''
        + ", favorite_number="
        + favorite_number
        + '}';
  }
}
