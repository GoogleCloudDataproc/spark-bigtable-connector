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

import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_INSTANCE_ID;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_PROJECT_ID;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_PUSH_DOWN_ROW_KEY_FILTERS;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterPushDownFuzzTest extends AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(FilterPushDownFuzzTest.class);
  private static BigtableTableAdminClient adminClient;

  private final int minRows = 50000;
  private final int maxRows = 250000;
  // We use a smaller minCols to increase the chances of reusing the same column in the same filter
  // multiple times.
  private final int minCols = 3;
  private final int maxCols = 20;
  private static final long totalDurationMinutes = 90;
  private final Stopwatch totalRunTime;

  public FilterPushDownFuzzTest() {
    totalRunTime = Stopwatch.createStarted();
  }

  @BeforeClass
  public static void initialSetup() throws Exception {
    spark = createSparkSession();
    setBigtableProperties();

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();
    adminClient = BigtableTableAdminClient.create(adminSettings);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    adminClient.close();
  }

  @Test
  public void filterPushDownFuzzTest() throws Exception {
    // We support the option to create a pre-split table using Bigtable cli tool
    // before running the test to check filter intersection w/ tablet boundaries.
    boolean tableCreatedOutsideTest = (runtimeProvidedTableId != null);
    int nonEmptyResultFiltersCount = 0;
    try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {
      RandomDataFrameGenerator generator =
          new RandomDataFrameGenerator(spark, minRows, maxRows, minCols, maxCols);
      String useTable = tableCreatedOutsideTest ? runtimeProvidedTableId : generateTableId();
      String catalog = parameterizeCatalog(generator.getRawSchemaString(), useTable);
      LOG.info("Created catalog = " + catalog);
      writeDataframeToBigtable(generator.getDf(), catalog, !tableCreatedOutsideTest);

      do {
        Column filter = generator.generateRandomFilter(5, 0.5);
        LOG.info("Reading the table using filter " + filter);

        Dataset<org.apache.spark.sql.Row> readWithPushDown =
            readWithFilter(catalog, filter, "true");
        LOG.info("Finished reading DataFrame *with* row key filter push down");
        readWithPushDown.show();

        Dataset<org.apache.spark.sql.Row> readWithoutPushDown =
            readWithFilter(catalog, filter, "false");
        LOG.info("Finished reading DataFrame *without* row key filter push down");
        readWithoutPushDown.show();

        assertDataFramesEqualLocally(
            readWithPushDown, readWithoutPushDown, generator.getRowKeyCol());

        long numOfRowsRead = readWithPushDown.count();
        LOG.info("Total number of rows returned = " + numOfRowsRead);
        if (numOfRowsRead > 0) {
          nonEmptyResultFiltersCount++;
        }
      } while (totalRunTime.elapsed(TimeUnit.MINUTES) < totalDurationMinutes);
      assertTrue(nonEmptyResultFiltersCount > 5);

      LOG.info(
          "Successfully read and compared table "
              + useTable
              + " in all iterations, deleting it now.");
      // The shell script will delete the table if it's created one, no need to delete it here.
      if (!tableCreatedOutsideTest) {
        deleteBigtableTable(useTable, adminClient);
      }
    }
  }

  private Dataset<Row> readWithFilter(
      String catalog, Column filterCondition, String pushDownRowKeyFilters) {
    return spark
        .read()
        .format("bigtable")
        .option("catalog", catalog)
        .option(BIGTABLE_PROJECT_ID(), projectId)
        .option(BIGTABLE_INSTANCE_ID(), instanceId)
        .option(BIGTABLE_PUSH_DOWN_ROW_KEY_FILTERS(), pushDownRowKeyFilters)
        .load()
        .filter(filterCondition);
  }

  String testName() {
    return "filter-fuzz";
  }
}
