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

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import java.util.ArrayList;
import junitparams.JUnitParamsRunner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnitParamsRunner.class)
public class ReadWriteLongRunningTest extends AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteLongRunningTest.class);

  private static BigtableTableAdminClient adminClient;

  private static final int batchSize = 1000;
  private static final long batchGapMillis = 5 * 60 * 1000; // 5 minutes
  private static final long totalDurationMillis = 18 * 3600 * 1000; // 18 hours

  private long endTime;

  public ReadWriteLongRunningTest() {
    endTime = System.currentTimeMillis() + totalDurationMillis;
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
  public void writeAndReadLongRunningTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);
    int currentBatch = 0;
    ArrayList<Long> originalWriteTimes = new ArrayList<Long>();

    try {
      while (true) {
        Dataset<Row> df =
            createTestDataFrameWithOffset(
                currentBatch * batchSize, (currentBatch + 1) * batchSize, 0);

        writeDataframeToBigtable(df, catalog, false);
        LOG.info("Wrote batch #" + currentBatch + " to Bigtable.");
        originalWriteTimes.add(currentBatch, System.currentTimeMillis() + batchGapMillis / 2);

        if (System.currentTimeMillis() + batchGapMillis > endTime) {
          break;
        }
        Thread.sleep(batchGapMillis);

        if (currentBatch > 0) {
          Dataset<Row> additioanlDF =
              createTestDataFrameWithOffset(
                  (currentBatch - 1) * batchSize, currentBatch * batchSize, 1);

          writeDataframeToBigtable(additioanlDF, catalog, false);
          LOG.info("Wrote batch #" + (currentBatch - 1) + " to Bigtable again.");
        }

        Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog);
        LOG.info("DataFrame in batch #" + currentBatch + " was read from Bigtable.");

        currentBatch++;

        assertEquals(currentBatch * batchSize, readDf.rdd().count());

        int existingRowkeyNumber = currentBatch * batchSize / 2;
        int relevantBatch = existingRowkeyNumber / batchSize;
        // In case we are now checking a row from a batch that hasn't been updated yet (after batch
        // 0 we check a row from batch 0, after batch 1 we check a row from batch 1, after batch 2
        // we check a row from batch 1 and then this difference is only growing), we expect the
        // offset to be 0, and 1 otherwise.
        int offset = relevantBatch < originalWriteTimes.size() - 1 ? 1 : 0;

        LOG.info("Asserting latest data for key #" + existingRowkeyNumber);
        assertOnlyOne(spark, catalog, existingRowkeyNumber, offset);

        LOG.info("Asserting original data for key #" + existingRowkeyNumber);
        Long timeBeforeAdditionalWrite = originalWriteTimes.get(relevantBatch);
        assertOnlyOne(
            spark,
            catalog,
            existingRowkeyNumber,
            0,
            withReaderStartTime(0L),
            withReaderEndTime(timeBeforeAdditionalWrite));
      }
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  String testName() {
    return "long-running";
  }
}
