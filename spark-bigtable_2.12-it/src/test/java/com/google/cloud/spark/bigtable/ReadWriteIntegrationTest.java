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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.spark.bigtable.model.Favorites;
import com.google.cloud.spark.bigtable.model.TestAvroRow;
import com.google.cloud.spark.bigtable.model.TestRow;
import com.google.cloud.spark.bigtable.repackaged.com.google.api.gax.rpc.NotFoundException;
import java.util.ArrayList;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnitParamsRunner.class)
public class ReadWriteIntegrationTest extends AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteIntegrationTest.class);

  private static BigtableTableAdminClient adminClient;

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
  @Parameters({
    // String rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,\"rowkey\":\"stringCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"rowkey\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"}\\,\"stringCol2\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"}\\,\"byteCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"byteCol\"\\, \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\,"
        + " \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"col_family2\"\\, \"col\":\"shortCol\"\\,"
        + " \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\, \"col\":\"intCol\"\\,"
        + " \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"col_family2\"\\, \"col\":\"longCol\"\\,"
        + " \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"col_family3\"\\, \"col\":\"floatCol\"\\,"
        + " \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"col_family3\"\\, \"col\":\"doubleCol\"\\,"
        + " \"type\":\"double\"}}}",
    // Double rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,\"rowkey\":\"doubleCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"col_family1\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"}\\,\"stringCol2\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"}\\,\"byteCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"byteCol\"\\, \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\,"
        + " \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"col_family2\"\\, \"col\":\"shortCol\"\\,"
        + " \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\, \"col\":\"intCol\"\\,"
        + " \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"col_family2\"\\, \"col\":\"longCol\"\\,"
        + " \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"col_family3\"\\, \"col\":\"floatCol\"\\,"
        + " \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"rowkey\"\\, \"col\":\"doubleCol\"\\,"
        + " \"type\":\"double\"}}}",
    // Compound rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,"
        + "\"rowkey\":\"stringCol:stringCol2:shortCol:floatCol:longCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"rowkey\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"\\, \"length\":\"15\"}\\,\"stringCol2\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"\\,"
        + " \"length\":\"15\"}\\,\"byteCol\":{\"cf\":\"col_family1\"\\, \"col\":\"byteCol\"\\,"
        + " \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\, \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"shortCol\"\\, \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\,"
        + " \"col\":\"intCol\"\\, \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"longCol\"\\, \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"floatCol\"\\, \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"col_family3\"\\,"
        + " \"col\":\"doubleCol\"\\, \"type\":\"double\"}}}"
  })
  public void simpleWriteAndReadRowkeyTest(String rawCatalog) throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      Dataset<Row> df = createTestDataFrame(256);
      LOG.info("Original DataFrame Created.");

      String catalog = parameterizeCatalog(rawCatalog, useTable);
      writeDataframeToBigtable(df, catalog, false);

      Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog);
      LOG.info("DataFrame was read from Bigtable.");
      assertDataFramesEqual(readDf, df);
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  @Parameters({
    // String rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,\"rowkey\":\"stringCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"rowkey\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"}\\,\"stringCol2\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"}\\,\"byteCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"byteCol\"\\, \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\,"
        + " \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"col_family2\"\\, \"col\":\"shortCol\"\\,"
        + " \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\, \"col\":\"intCol\"\\,"
        + " \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"col_family2\"\\, \"col\":\"longCol\"\\,"
        + " \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"col_family3\"\\, \"col\":\"floatCol\"\\,"
        + " \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"col_family3\"\\, \"col\":\"doubleCol\"\\,"
        + " \"type\":\"double\"}}}",
    // Double rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,\"rowkey\":\"doubleCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"col_family1\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"}\\,\"stringCol2\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"}\\,\"byteCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"byteCol\"\\, \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\,"
        + " \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"col_family2\"\\, \"col\":\"shortCol\"\\,"
        + " \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\, \"col\":\"intCol\"\\,"
        + " \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"col_family2\"\\, \"col\":\"longCol\"\\,"
        + " \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"col_family3\"\\, \"col\":\"floatCol\"\\,"
        + " \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"rowkey\"\\, \"col\":\"doubleCol\"\\,"
        + " \"type\":\"double\"}}}",
    // Compound rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,"
        + "\"rowkey\":\"stringCol:stringCol2:shortCol:floatCol:longCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"rowkey\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"\\, \"length\":\"15\"}\\,\"stringCol2\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"\\,"
        + " \"length\":\"15\"}\\,\"byteCol\":{\"cf\":\"col_family1\"\\, \"col\":\"byteCol\"\\,"
        + " \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\, \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"shortCol\"\\, \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\,"
        + " \"col\":\"intCol\"\\, \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"longCol\"\\, \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"floatCol\"\\, \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"col_family3\"\\,"
        + " \"col\":\"doubleCol\"\\, \"type\":\"double\"}}}"
  })
  public void simpleWriteAndReadFilteredRowkeyTest(String rawCatalog) throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      Dataset<Row> df = createTestDataFrame(256);
      LOG.info("Original DataFrame Created.");

      String catalog = parameterizeCatalog(rawCatalog, useTable);
      writeDataframeToBigtable(df, catalog, false);

      Dataset<Row> shouldBeEmpty =
          readDataframeFromBigtable(spark, catalog, "stringCol = 'not present'");
      LOG.info("DataFrame was read from Bigtable.");
      assertTrue(shouldBeEmpty.rdd().isEmpty());
      assertEquals(0, shouldBeEmpty.rdd().count());

      Dataset<Row> shouldBeOne =
          readDataframeFromBigtable(spark, catalog, "stringCol = 'StringColOne000'");
      LOG.info("DataFrame was read from Bigtable.");

      assertEquals(1, shouldBeOne.rdd().count());
      assertFalse(shouldBeOne.rdd().isEmpty());

      TestRow rowWeShouldHaveSelected = generateTestRow(-128, -128);

      shouldBeOne.foreach(
          r -> {
            assertEquals(
                rowWeShouldHaveSelected.getStringCol(), r.getString(r.fieldIndex("stringCol")));
            assertEquals(
                rowWeShouldHaveSelected.getStringCol2(), r.getString(r.fieldIndex("stringCol2")));
            assertEquals(rowWeShouldHaveSelected.getByteCol(), r.getByte(r.fieldIndex("byteCol")));
            assertEquals(rowWeShouldHaveSelected.getLongCol(), r.getLong(r.fieldIndex("longCol")));
            assertEquals(rowWeShouldHaveSelected.getIntCol(), r.getInt(r.fieldIndex("intCol")));
            assertEquals(
                rowWeShouldHaveSelected.getShortCol(), r.getShort(r.fieldIndex("shortCol")));
          });
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  // This test is to check against the breaking point (i.e., 0) of long row keys.
  @Test
  @Parameters({
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,\"rowkey\":\"byteCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"col_family1\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"}\\,\"stringCol2\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"}\\,\"byteCol\":{\"cf\":\"rowkey\"\\,"
        + " \"col\":\"byteCol\"\\, \"type\":\"byte\"}\\,\"booleanCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"booleanCol\"\\,"
        + " \"type\":\"boolean\"}\\,\"shortCol\":{\"cf\":\"col_family2\"\\, \"col\":\"shortCol\"\\,"
        + " \"type\":\"short\"}\\,\"intCol\":{\"cf\":\"col_family2\"\\, \"col\":\"intCol\"\\,"
        + " \"type\":\"int\"}\\,\"longCol\":{\"cf\":\"col_family2\"\\, \"col\":\"longCol\"\\,"
        + " \"type\":\"long\"}\\,\"floatCol\":{\"cf\":\"col_family3\"\\, \"col\":\"floatCol\"\\,"
        + " \"type\":\"float\"}\\,\"doubleCol\":{\"cf\":\"col_family3\"\\, \"col\":\"doubleCol\"\\,"
        + " \"type\":\"double\"}}}",
  })
  public void simpleWriteAndReadFilterOnByteRowkeyTest(String rawCatalog) throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      Dataset<Row> df = createTestDataFrame(256);
      LOG.info("Original DataFrame Created.");

      String catalog = parameterizeCatalog(rawCatalog, useTable);
      writeDataframeToBigtable(df, catalog, false);

      Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog, "byteCol < 20");
      LOG.info("DataFrame was read from Bigtable.");
      // 128 rows w/ negative values and 20 non-negative ones.
      assertEquals(128 + 20, readDf.rdd().count());
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  // This test is to check against intersecting filters containing ranges and points.
  @Test
  @Parameters({
    "((intCol = 20) OR ((intCol >= 1) AND (intCol <= 15))) AND (longCol < 1000), 16",
    // The two points from each of the main AND branches (i.e., 40/50 or 10/15)
    // intersect with the range of the other branch
    "((intCol >= 1 AND intCol < 21) OR (intCol = 40 OR intCol = 50))"
        + " AND ((intCol >= 30 AND intCol < 51) OR (intCol = 10 OR intCol = 15)), 4"
  })
  public void simpleWriteAndReadPointWithRangeIntersectionsTest(String filter, int expectedRowCount)
      throws Exception {
    String rawCatalog =
        "{\"table\":{\"name\":\"${tablename}\","
            + "\"tableCoder\":\"PrimitiveType\"},\"rowkey\":\"intCol\","
            + "\"columns\":{\"stringCol\":{\"cf\":\"col_family1\", \"col\":\"stringCol\","
            + " \"type\":\"string\"},\"stringCol2\":{\"cf\":\"col_family1\","
            + " \"col\":\"stringCol2\", \"type\":\"string\"},\"byteCol\":{\"cf\":\"col_family1\","
            + " \"col\":\"byteCol\", \"type\":\"byte\"},\"booleanCol\":{\"cf\":\"col_family1\","
            + " \"col\":\"booleanCol\","
            + " \"type\":\"boolean\"},\"shortCol\":{\"cf\":\"col_family2\", \"col\":\"shortCol\","
            + " \"type\":\"short\"},\"intCol\":{\"cf\":\"rowkey\", \"col\":\"intCol\","
            + " \"type\":\"int\"},\"longCol\":{\"cf\":\"col_family2\", \"col\":\"longCol\","
            + " \"type\":\"long\"},\"floatCol\":{\"cf\":\"col_family3\", \"col\":\"floatCol\","
            + " \"type\":\"float\"},\"doubleCol\":{\"cf\":\"col_family3\", \"col\":\"doubleCol\","
            + " \"type\":\"double\"}}}";
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      Dataset<Row> df = createTestDataFrame(256);
      LOG.info("Original DataFrame Created.");

      String catalog = parameterizeCatalog(rawCatalog, useTable);
      writeDataframeToBigtable(df, catalog, false);

      Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog, filter);
      LOG.info("DataFrame was read from Bigtable.");
      assertEquals(expectedRowCount, readDf.rdd().count());
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  @Parameters({
    // String rowkey
    "{\"table\":{\"name\":\"${tablename}\"\\,"
        + "\"tableCoder\":\"PrimitiveType\"}\\,\"rowkey\":\"stringCol\"\\,"
        + "\"columns\":{\"stringCol\":{\"cf\":\"rowkey\"\\, \"col\":\"stringCol\"\\,"
        + " \"type\":\"string\"}\\,\"stringCol2\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"stringCol2\"\\, \"type\":\"string\"}\\,\"avroCol\":{\"cf\":\"col_family1\"\\,"
        + " \"col\":\"avroCol\"\\, \"avro\":\"avroSchema\"}}}"
  })
  public void simpleWriteAndReadAvroDataTest(String rawCatalog) throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      Dataset<Row> df = createTestAvroDataFrame(256);

      df.show();

      LOG.info("Original DataFrame Created.");

      String catalog = parameterizeCatalog(rawCatalog, useTable);

      String avroSchemaString =
          "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"Favorites\", "
              + "\"fields\": [ "
              + "{\"name\": \"favorite_number\", \"type\": \"int\"}, "
              + "{\"name\": \"favorite_string\", \"type\": \"string\"}"
              + "]}";

      GenericDatumWriter s;

      writeDataframeToBigtable(df, catalog, false, withWriterAvroSchema(avroSchemaString));

      Dataset<Row> shouldBeEmpty =
          readDataframeFromBigtable(
              spark,
              catalog,
              "avroCol.favorite_string = 'not present'",
              withReaderAvroSchema(avroSchemaString));

      LOG.info("DataFrame was read from Bigtable.");
      assertTrue(shouldBeEmpty.rdd().isEmpty());
      assertEquals(0, shouldBeEmpty.rdd().count());

      Dataset<Row> shouldBeOne =
          readDataframeFromBigtable(
              spark,
              catalog,
              "avroCol.favorite_string = 'boo1'",
              withReaderAvroSchema(avroSchemaString));
      LOG.info("DataFrame was read from Bigtable.");

      assertEquals(1, shouldBeOne.rdd().count());
      assertFalse(shouldBeOne.rdd().isEmpty());

      TestAvroRow rowWeShouldHaveSelected = generateTestAvroRow(1);

      shouldBeOne.foreach(
          r -> {
            assertEquals(
                rowWeShouldHaveSelected.getStringCol(), r.getString(r.fieldIndex("stringCol")));
            assertEquals(
                rowWeShouldHaveSelected.getStringCol2(), r.getString(r.fieldIndex("stringCol2")));
            assertEquals(
                rowWeShouldHaveSelected.getAvroCol(),
                ReadWriteIntegrationTest.asFavorites(
                    (GenericRowWithSchema) r.getAs(r.fieldIndex("avroCol"))));
          });
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  public void writeAndReadLargeTableTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      Dataset<Row> df = createTestDataFrame(100_000);

      String catalog = parameterizeCatalog(rawBasicCatalog, useTable);
      writeDataframeToBigtable(df, catalog, false);

      Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog);
      LOG.info("DataFrame was read from Bigtable.");
      assertDataFramesEqual(readDf, df);
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  public void createNewTableWithSparkTest() throws Exception {
    String useTable = generateTableId();

    try {
      Dataset<Row> df = createTestDataFrame(256);

      String catalog = parameterizeCatalog(rawBasicCatalog, useTable);
      writeDataframeToBigtable(df, catalog, true);
      Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog);
      LOG.info("DataFrame was read from Bigtable.");
      assertDataFramesEqual(readDf, df);
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  public void readEmptyTableWithoutWritingTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      String catalog = parameterizeCatalog(rawBasicCatalog, useTable);
      Dataset<Row> shouldBeEmpty = readDataframeFromBigtable(spark, catalog);
      assertTrue(shouldBeEmpty.rdd().isEmpty());
      assertEquals(0, shouldBeEmpty.rdd().count());
      LOG.info("Empty DataFrame was read from Bigtable.");
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  public void writeAndReadWithDifferentInstancesTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      // We first read from an existing instance and then from a non-existing
      // one, to ensure that connection caching won't break the second read.
      String catalog = parameterizeCatalog(rawBasicCatalog, useTable);
      Dataset<Row> shouldBeEmpty = readDataframeFromBigtable(spark, catalog);
      assertTrue(shouldBeEmpty.rdd().isEmpty());
      LOG.info("DataFrame was read from the correct Bigtable instance.");

      Dataset<Row> readDfWithError =
          readDataframeFromBigtable(
              spark, catalog, "", withReaderOverwritingInstanceId("nonexistent-instance"));
      try {
        readDfWithError.show();
        fail("Should have receieved an exception.");
      } catch (NotFoundException e) {
        LOG.debug("Correctly threw NotFoundException for nonexistent instance.");
      }
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  public void writeAndReadWithUsingTimeRangesTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

      Long beforeFirst = System.currentTimeMillis();
      Dataset<Row> df = createTestDataFrameWithOffset(0, 100, 0);
      writeDataframeToBigtable(df, catalog, false);
      Long afterFirst = System.currentTimeMillis();

      df = createTestDataFrameWithOffset(0, 100, 1);
      writeDataframeToBigtable(df, catalog, false);
      Long afterSecond = System.currentTimeMillis();

      int rowkeyNumber = 50;

      assertEmpty(
          spark, catalog, rowkeyNumber, withReaderStartTime(0L), withReaderEndTime(beforeFirst));
      assertOnlyOne(
          spark,
          catalog,
          rowkeyNumber,
          /* offset= */ 0,
          withReaderStartTime(beforeFirst),
          withReaderEndTime(afterFirst));
      assertEmpty(
          spark,
          catalog,
          rowkeyNumber,
          withReaderStartTime(afterSecond),
          withReaderEndTime(afterSecond + 1));
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  private static Favorites asFavorites(GenericRowWithSchema result) {
    Favorites fav = new Favorites();
    fav.setFavoriteNumber(result.getInt(result.fieldIndex("favorite_number")));
    fav.setFavoriteString(result.getString(result.fieldIndex("favorite_string")));
    return fav;
  }

  private TestAvroRow generateTestAvroRow(int number) {
    return new TestAvroRow(
        String.format("StringColOne%03d", number),
        String.format("StringColTwo%03d", number),
        new Favorites("boo" + number, number));
  }

  private Dataset<Row> createTestAvroDataFrame(int numOfRows) {
    ArrayList<TestAvroRow> rows = new ArrayList<>();
    for (int i = 0; i < numOfRows; i++) {
      rows.add(generateTestAvroRow(i));
    }

    Dataset<Row> df = spark.createDataset(rows, Encoders.bean(TestAvroRow.class)).toDF();

    return df;
  }

  String testName() {
    return "integration";
  }
}
