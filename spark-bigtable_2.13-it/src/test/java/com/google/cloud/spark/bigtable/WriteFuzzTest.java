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

import static org.junit.Assert.assertEquals;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.spark.bigtable.datasources.BytesConverter;
import com.google.common.base.Stopwatch;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteFuzzTest extends AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(WriteFuzzTest.class);
  private static BigtableTableAdminClient adminClient;

  private final int minRows = 50000;
  private final int maxRows = 250000;
  private final int minCols = 18;
  private final int maxCols = 25;
  private static final long totalDurationMinutes = 90;;
  private final Stopwatch totalRunTime;

  public WriteFuzzTest() {
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
    stopSparkSession(spark);
  }

  @Test
  public void writeFuzzTest() throws Exception {
    do {
      RandomDataFrameGenerator generator =
          new RandomDataFrameGenerator(spark, minRows, maxRows, minCols, maxCols);
      String useTable = generateTableId();
      String catalog = parameterizeCatalog(generator.getRawSchemaString(), useTable);
      LOG.info("Created catalog = " + catalog);
      writeDataframeToBigtable(generator.getDf(), catalog, true);

      try (BigtableDataClient dataClient = BigtableDataClient.create(projectId, instanceId)) {
        ServerStream<Row> rows = dataClient.readRows(Query.create(useTable));
        List<org.apache.spark.sql.Row> readRows = new ArrayList<>();
        for (Row btRow : rows) {
          List<Object> rowColumns = new ArrayList<>();
          for (int i = 0; i < generator.getNumOfCols(); i++) {
            StructField colField = generator.getSchema().apply(i);
            byte[] valueBytes;
            if (colField.name().equals(generator.getRowKeyCol())) {
              valueBytes = btRow.getKey().toByteArray();
            } else {
              List<RowCell> allCells = btRow.getCells(generator.colFamilyName, colField.name());
              assertEquals(allCells.size(), 1);
              valueBytes = allCells.get(0).getValue().toByteArray();
            }
            DataType dataType = colField.dataType();
            rowColumns.add(getDataFrameValue(valueBytes, dataType));
          }
          readRows.add(RowFactory.create(rowColumns.toArray()));
        }

        Dataset<org.apache.spark.sql.Row> readDf =
            spark.createDataFrame(readRows, generator.getSchema());
        LOG.info("Finished reading DataFrame from Bigtable");
        readDf.show();

        assertDataFramesEqualLocally(readDf, generator.getDf(), generator.getRowKeyCol());
        LOG.info("Successfully read and compared table " + useTable + ", deleting it now.");
        deleteBigtableTable(useTable, adminClient);
      }
    } while (totalRunTime.elapsed(TimeUnit.MINUTES) < totalDurationMinutes);
  }

  private Object getDataFrameValue(byte[] valueBytes, DataType dataType) {
    if (dataType.equals(DataTypes.BooleanType)) {
      return BytesConverter.toBoolean(valueBytes, 0);
    } else if (dataType.equals(DataTypes.ByteType)) {
      return valueBytes[0];
    } else if (dataType.equals(DataTypes.ShortType)) {
      return BytesConverter.toShort(valueBytes, 0);
    } else if (dataType.equals(DataTypes.IntegerType)) {
      return BytesConverter.toInt(valueBytes, 0);
    } else if (dataType.equals(DataTypes.LongType)) {
      return BytesConverter.toLong(valueBytes, 0);
    } else if (dataType.equals(DataTypes.FloatType)) {
      return BytesConverter.toFloat(valueBytes, 0);
    } else if (dataType.equals(DataTypes.DoubleType)) {
      return BytesConverter.toDouble(valueBytes, 0);
    } else if (dataType.equals(DataTypes.TimestampType)) {
      return new Timestamp(BytesConverter.toLong(valueBytes, 0));
    } else if (dataType.equals(DataTypes.StringType)) {
      return BytesConverter.toString(valueBytes, 0);
    } else if (dataType.equals(DataTypes.BinaryType)) {
      return valueBytes;
    } else {
      throw new IllegalStateException("Unexpected column type " + dataType);
    }
  }

  String testName() {
    return "write-fuzz";
  }
}
