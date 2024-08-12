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
package com.google.cloud.spark.bigtable;

import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.DEFAULT_BIGTABLE_APP_PROFILE_ID;
import static org.junit.Assert.assertEquals;

import junitparams.JUnitParamsRunner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class DataBoostIntegrationTest extends AbstractTestBase {
  // We use a fixed table to test Data Boost so that we wouldn't have to wait
  // for a compaction each time we're running the test.
  private static final String tableName = "DO-NOT-DELETE-cbt-data-boost";
  private static final int numOfRows = 256;
  private static final String dataBoostAppProfile = "data-boost";

  @Test
  public void writeAndReadLongRunningTest() {
    String catalog = parameterizeCatalog(rawBasicCatalog, tableName);
    Dataset<Row> expectedDf = createTestDataFrame(numOfRows);

    Dataset<Row> readDfDefaultAppProfile =
        readDataframeFromBigtable(
            spark, catalog, withReaderAppProfile(DEFAULT_BIGTABLE_APP_PROFILE_ID()));
    assertEquals(readDfDefaultAppProfile.rdd().count(), expectedDf.rdd().count());
    assertDataFramesEqual(readDfDefaultAppProfile, expectedDf);

    Dataset<Row> readDfDataBoostAppProfile =
        readDataframeFromBigtable(spark, catalog, withReaderAppProfile(dataBoostAppProfile));
    assertEquals(readDfDataBoostAppProfile.rdd().count(), expectedDf.rdd().count());
    assertDataFramesEqual(readDfDataBoostAppProfile, expectedDf);
  }

  String testName() {
    return "data-boost";
  }
}
