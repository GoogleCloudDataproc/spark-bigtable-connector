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

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_APP_PROFILE_ID;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_BATCH_MUTATE_SIZE;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_CREATE_NEW_TABLE;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_EMULATOR_PORT;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_INSTANCE_ID;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_PROJECT_ID;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_TIMERANGE_END;
import static com.google.cloud.spark.bigtable.datasources.BigtableSparkConf.BIGTABLE_TIMERANGE_START;
import static org.apache.spark.sql.functions.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules.VersionRule;
import com.google.cloud.spark.bigtable.model.TestRow;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestBase.class);

  static final String PROJECT_ID_PROPERTY_NAME = "bigtableProjectId";
  static final String INSTANCE_ID_PROPERTY_NAME = "bigtableInstanceId";
  static final String TABLE_ID_PROPERTY_NAME = "bigtableTableId";

  static String projectId;
  static String instanceId;
  static String runtimeProvidedTableId;
  static SparkSession spark;

  static JavaSparkContext javaSparkContext;

  // This basic catalog is used for tests where we are not specifically testing
  // catalog functionalities.
  final String rawBasicCatalog =
      "{\"table\":{\"name\":\"${tablename}\","
          + "\"tableCoder\":\"PrimitiveType\"},\"rowkey\":\"stringCol\","
          + "\"columns\":{\"stringCol\":{\"cf\":\"rowkey\", \"col\":\"stringCol\","
          + " \"type\":\"string\"},\"stringCol2\":{\"cf\":\"col_family1\","
          + " \"col\":\"stringCol2\", \"type\":\"string\"},\"byteCol\":{\"cf\":\"col_family1\","
          + " \"col\":\"byteCol\", \"type\":\"byte\"},\"booleanCol\":{\"cf\":\"col_family1\","
          + " \"col\":\"booleanCol\", \"type\":\"boolean\"},\"shortCol\":{\"cf\":\"col_family2\","
          + " \"col\":\"shortCol\", \"type\":\"short\"},\"intCol\":{\"cf\":\"col_family2\","
          + " \"col\":\"intCol\", \"type\":\"int\"},\"longCol\":{\"cf\":\"col_family2\","
          + " \"col\":\"longCol\", \"type\":\"long\"},\"floatCol\":{\"cf\":\"col_family3\","
          + " \"col\":\"floatCol\", \"type\":\"float\"},\"doubleCol\":{\"cf\":\"col_family3\","
          + " \"col\":\"doubleCol\", \"type\":\"double\"}}}";

  static SparkSession createSparkSession() {
    return SparkSession.builder().master("local").config("spark.ui.enabled", "false").getOrCreate();
  }

  static JavaSparkContext createJavaSparkContext() {
    spark = createSparkSession();
    return new JavaSparkContext(spark.sparkContext());
  }

  static void setBigtableProperties() throws Exception {
    projectId = System.getProperty(PROJECT_ID_PROPERTY_NAME);
    instanceId = System.getProperty(INSTANCE_ID_PROPERTY_NAME);
    if (instanceId == null || projectId == null) {
      throw new IllegalArgumentException(
          "Bigtable project and instance ID must be provided using "
              + "-D"
              + PROJECT_ID_PROPERTY_NAME
              + "=X and "
              + "-D"
              + INSTANCE_ID_PROPERTY_NAME
              + "=Y, respectively.");
    }
    runtimeProvidedTableId = System.getProperty(TABLE_ID_PROPERTY_NAME);
  }

  void assertDataFramesEqual(Dataset<Row> actualDf, Dataset<Row> expectedDf) {
    String[] actualColumns = actualDf.columns();
    String[] expectedColumns = expectedDf.columns();
    assertEquals(expectedColumns.length, actualColumns.length);
    // Sort columns to ensure having the same order when picking the first
    Arrays.sort(actualColumns);
    Arrays.sort(expectedColumns);
    for (int i = 0; i < expectedColumns.length; i++) {
      assertEquals(expectedColumns[i], actualColumns[i]);
    }

    Dataset<Row> actualAggregated = actualDf.agg(count(actualColumns[0]));
    Dataset<Row> expectedAggregated = expectedDf.agg(count(expectedColumns[0]));

    assertTrue(
        actualAggregated.except(expectedAggregated).rdd().isEmpty()
            && expectedAggregated.except(actualAggregated).rdd().isEmpty());
  }

  // This method compares each cell value and is more accurate, but is less efficient
  // since it collects the DataFrames locally.
  void assertDataFramesEqualLocally(
      Dataset<Row> actualDf, Dataset<Row> expectedDf, String rowKeyColumn) throws Exception {
    assertEquals(expectedDf.schema(), actualDf.schema());
    String[] actualColumns = actualDf.columns();
    String[] expectedColumns = expectedDf.columns();

    for (int i = actualColumns.length - 1; i >= 0; i--) {
      actualDf = actualDf.sort(rowKeyColumn);
      expectedDf = expectedDf.sort(rowKeyColumn);
    }
    List<Row> actualRows = actualDf.collectAsList();
    List<Row> expectedRows = expectedDf.collectAsList();

    assertEquals(expectedRows.size(), actualRows.size());

    boolean dataFramesEqual = true;
    for (int i = 0; i < actualRows.size(); i++) {
      Row actualRow = actualRows.get(i);
      Row expectedRow = expectedRows.get(i);
      for (int j = 0; j < actualColumns.length; j++) {
        Object actualValue = actualRow.getAs(actualColumns[j]);
        Object expectedValue = expectedRow.getAs(expectedColumns[j]);

        if (!areValuesEqual(actualValue, expectedValue)) {
          LOG.error(
              "Error when comparing actual row "
                  + actualRow
                  + " with expected row "
                  + expectedRow
                  + ". Actual value "
                  + actualValue
                  + " does not equal expected value "
                  + expectedValue
                  + ".");
          dataFramesEqual = false;
        }
      }
    }
    assertTrue(dataFramesEqual);
  }

  private boolean areValuesEqual(Object actualValue, Object expectedValue) {
    if (actualValue instanceof byte[]) {
      return Arrays.equals((byte[]) actualValue, (byte[]) expectedValue);
    } else {
      return actualValue.equals(expectedValue);
    }
  }

  void createBigtableTable(String tableName, BigtableTableAdminClient adminClient)
      throws Exception {
    // Some tests assert previous versions. Default MaxVersions is 1, so these tests fail.
    VersionRule versionRule = GCRULES.maxVersions(3);
    if (!adminClient.exists(tableName)) {
      CreateTableRequest createTableRequest =
          CreateTableRequest.of(tableName)
              .addFamily("col_family1", versionRule)
              .addFamily("col_family2", versionRule)
              .addFamily("col_family3", versionRule);
      adminClient.createTable(createTableRequest);
      LOG.info("Created a CBT table: " + tableName);
    }
  }

  void deleteBigtableTable(String tableName, BigtableTableAdminClient adminClient) {
    try {
      adminClient.deleteTable(tableName);
    } catch (Exception e) {
      LOG.warn("Failed to delete the table: " + tableName, e);
    }
  }

  void writeDataframeToBigtable(
      Dataset<Row> dataframe, String catalog, boolean createNewTable, WriterOption... options) {
    DataFrameWriter<Row> dataframeWriter =
        dataframe
            .write()
            .format("bigtable")
            .option("catalog", catalog)
            .option(BIGTABLE_PROJECT_ID(), projectId)
            .option(BIGTABLE_INSTANCE_ID(), instanceId)
            .option(BIGTABLE_CREATE_NEW_TABLE(), createNewTable);

    for (WriterOption option : options) {
      dataframeWriter = option.writeOp(dataframeWriter);
    }
    dataframeWriter.save();
  }

  interface ReaderOption {
    public DataFrameReader readOp(DataFrameReader dataframeReader);
  }

  interface WriterOption {
    public DataFrameWriter<Row> writeOp(DataFrameWriter<Row> dataframeWriter);
  }

  static WriterOption withWriterAvroSchema(String avroSchema) {
    return writer -> writer.option("avroSchema", avroSchema);
  }

  static WriterOption withWriterEmulatorPort(String emulatorPort) {
    return writer -> writer.option(BIGTABLE_EMULATOR_PORT(), emulatorPort);
  }

  static WriterOption withWriterBatchMutateSize(String batchSize) {
    return writer -> writer.option(BIGTABLE_BATCH_MUTATE_SIZE(), batchSize);
  }

  static ReaderOption withReaderAvroSchema(String avroSchema) {
    return reader -> reader.option("avroSchema", avroSchema);
  }

  static ReaderOption withReaderStartTime(Long timeInMilliseconds) {
    return reader -> reader.option(BIGTABLE_TIMERANGE_START(), timeInMilliseconds);
  }

  static ReaderOption withReaderEndTime(Long timeInMilliseconds) {
    return reader -> reader.option(BIGTABLE_TIMERANGE_END(), timeInMilliseconds);
  }

  static ReaderOption withReaderAppProfile(String appProfile) {
    return reader -> reader.option(BIGTABLE_APP_PROFILE_ID(), appProfile);
  }

  static ReaderOption withReaderEmulatorPort(String emulatorPort) {
    return reader -> reader.option(BIGTABLE_EMULATOR_PORT(), emulatorPort);
  }

  static ReaderOption withReaderMaxRetries(String maxRetries) {
    // The BigtableSparkConf.MAX_READ_ROWS_RETRIES attribute is private, so we manually specify it.
    return reader -> reader.option("spark.bigtable.max.read.rows.retries", maxRetries);
  }

  static ReaderOption withReaderOverwritingInstanceId(String newInstanceId) {
    return reader -> reader.option(BIGTABLE_INSTANCE_ID(), newInstanceId);
  }

  Dataset<Row> readDataframeFromBigtable(
      SparkSession spark, String catalog, ReaderOption... options) {
    return readDataframeFromBigtable(spark, catalog, "", options);
  }

  Dataset<Row> readDataframeFromBigtable(
      SparkSession spark, String catalog, String filterCondition, ReaderOption... options) {
    DataFrameReader dataframeReader =
        spark
            .read()
            .format("bigtable")
            .option("catalog", catalog)
            .option(BIGTABLE_PROJECT_ID(), projectId)
            .option(BIGTABLE_INSTANCE_ID(), instanceId);
    for (ReaderOption option : options) {
      dataframeReader = option.readOp(dataframeReader);
    }

    Dataset<Row> dataframe = dataframeReader.load();
    if (filterCondition != null && !filterCondition.isEmpty()) {
      dataframe = dataframe.filter(filterCondition);
    }
    return dataframe;
  }

  String parameterizeCatalog(String catalogTemplate, String cbtTable) {
    return catalogTemplate.replaceAll("\\s+", "").replaceAll("\\$\\{tablename\\}", cbtTable);
  }

  TestRow generateTestRow(int number, int startRange) {
    return new TestRow(
        // Keep the row number in the String non-negative.
        String.format("StringColOne%03d", number - startRange),
        String.format("StringColTwo%03d", number - startRange),
        (number % 2 == 0),
        (byte) number,
        (short) number,
        number,
        number,
        (float) (number * 3.14),
        (double) (number / 3.14));
  }

  TestRow generateTestRowWithOffset(int rowkeyNumber, int offset) {
    return new TestRow(
        String.format("StringColOne%08d", rowkeyNumber),
        String.format("StringColTwo%08d", rowkeyNumber),
        (rowkeyNumber % 2 == 0),
        (byte) rowkeyNumber,
        (short) rowkeyNumber,
        rowkeyNumber + offset,
        rowkeyNumber + offset,
        (float) (rowkeyNumber * 3.14),
        (double) (rowkeyNumber / 3.14));
  }

  Dataset<Row> createTestDataFrame(int numOfRows) {
    int startRange = -(numOfRows / 2);
    int endRange = (numOfRows + 1) / 2;
    ArrayList<TestRow> rows = new ArrayList<>();
    for (int i = startRange; i < endRange; i++) {
      rows.add(generateTestRow(i, startRange));
    }
    Dataset<Row> df = spark.createDataset(rows, Encoders.bean(TestRow.class)).toDF();

    return df;
  }

  Dataset<Row> createTestDataFrameWithOffset(int startRange, int endRange, int offset) {
    ArrayList<TestRow> rows = new ArrayList<>();
    for (int i = startRange; i < endRange; i++) {
      rows.add(generateTestRowWithOffset(i, offset));
    }
    Dataset<Row> df = spark.createDataset(rows, Encoders.bean(TestRow.class)).toDF();

    return df;
  }

  void assertOnlyOne(
      SparkSession spark,
      String catalog,
      int existingRowkeyNumber,
      int offset,
      ReaderOption... options) {
    Dataset<Row> shouldBeOne =
        readDataframeFromBigtable(
            spark,
            catalog,
            String.format("stringCol = 'StringColOne%08d'", existingRowkeyNumber),
            options);

    assertEquals(1, shouldBeOne.rdd().count());
    assertFalse(shouldBeOne.rdd().isEmpty());
    TestRow rowWeShouldHaveSelected = generateTestRowWithOffset(existingRowkeyNumber, offset);

    shouldBeOne.foreach(
        r -> {
          assertEquals(
              rowWeShouldHaveSelected.getStringCol(), r.getString(r.fieldIndex("stringCol")));
          assertEquals(
              rowWeShouldHaveSelected.getStringCol2(), r.getString(r.fieldIndex("stringCol2")));
          assertEquals(rowWeShouldHaveSelected.getByteCol(), r.getByte(r.fieldIndex("byteCol")));
          assertEquals(rowWeShouldHaveSelected.getLongCol(), r.getLong(r.fieldIndex("longCol")));
          assertEquals(rowWeShouldHaveSelected.getIntCol(), r.getInt(r.fieldIndex("intCol")));
          assertEquals(rowWeShouldHaveSelected.getShortCol(), r.getShort(r.fieldIndex("shortCol")));
        });
  }

  void assertEmpty(
      SparkSession spark, String catalog, int existingRowkeyNumber, ReaderOption... options) {
    Dataset<Row> shouldBeEmpty =
        readDataframeFromBigtable(
            spark,
            catalog,
            String.format("stringCol = 'StringColOne%08d'", existingRowkeyNumber),
            options);

    assertEquals(0, shouldBeEmpty.rdd().count());
    assertTrue(shouldBeEmpty.isEmpty());
  }

  abstract String testName();

  String generateTableId() {
    return "cbt-" + testName() + "-" + (UUID.randomUUID().toString().substring(0, 20));
  }
}
