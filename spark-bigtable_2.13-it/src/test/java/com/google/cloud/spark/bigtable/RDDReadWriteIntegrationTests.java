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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.spark.bigtable.datasources.BigtableSparkConf;
import com.google.cloud.spark.bigtable.datasources.BigtableSparkConfBuilder;
//import com.google.cloud.spark.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.Row;
//import com.google.cloud.spark.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowCell;
//import com.google.cloud.spark.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
//import com.google.cloud.spark.bigtable.repackaged.io.grpc.Status;
import /*com.google.cloud.spark.bigtable.repackaged.*/com.google.cloud.bigtable.data.v2.models.Row;
import /*com.google.cloud.spark.bigtable.repackaged.*/com.google.cloud.bigtable.data.v2.models.RowCell;
import /*com.google.cloud.spark.bigtable.repackaged.*/com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import /*com.google.cloud.spark.bigtable.repackaged.*/io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import junitparams.JUnitParamsRunner;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

@RunWith(JUnitParamsRunner.class)
public class RDDReadWriteIntegrationTests extends AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(RDDReadWriteIntegrationTests.class);

  private static final String COLUMN_FAMILY = "col_family1";
  private static final String NONEXISTENT_COLUMN_FAMILY = "nonexistent_col_family";
  private static final String COLUMN_QUALIFIER = "longCol";

  private static BigtableTableAdminClient adminClient;

  @BeforeClass
  public static void initialSetup() throws Exception {
    javaSparkContext = createJavaSparkContext();
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
    stopJavaSparkContext(javaSparkContext);
  }

  @Test
  public void simpleWriteAndReadTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      JavaRDD<Tuple2<String, Long>> tupleRDD = createTestRDD(256);
      JavaRDD<RowMutationEntry> mutationsRDD = convertTupleRDDToMutationsRDD(tupleRDD);
      LOG.info("Original RDD Created.");

      BigtableRDD bigtableRDD = new BigtableRDD(spark.sparkContext());
      bigtableRDD.writeRDD(mutationsRDD.rdd(), useTable, createRDDConf());

      JavaRDD<Row> bigtableRowsRDD = bigtableRDD.readRDD(useTable, createRDDConf()).toJavaRDD();

      JavaRDD<Tuple2<String, Long>> readTupleRDD =
          bigtableRowsRDD.map(
              bigtableRow -> {
                String rowKey = bigtableRow.getKey().toStringUtf8();
                Long columnValue = null;
                for (RowCell cell : bigtableRow.getCells(COLUMN_FAMILY, COLUMN_QUALIFIER)) {
                  columnValue = Long.parseLong(cell.getValue().toStringUtf8());
                }
                return new Tuple2<>(rowKey, columnValue);
              });

      LOG.info("RDD was read from Bigtable.");
      assertTupleRDDsEqual(readTupleRDD, tupleRDD);
    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  @Test
  public void writeWithFailingMutationTest() throws Exception {
    String useTable = generateTableId();
    createBigtableTable(useTable, adminClient);

    try {
      JavaRDD<Tuple2<String, Long>> tupleRDD = createTestRDD(256);
      // We split the RDD into two partitions with a single erroneous mutation
      // to verify that the entire operation still fails.
      JavaRDD<RowMutationEntry> erroneousMutationsRDD =
          convertTupleRDDToErroneousMutationsRDD(tupleRDD).repartition(2);
      LOG.info("Original RDD Created.");

      BigtableRDD bigtableRDD = new BigtableRDD(spark.sparkContext());
      try {
        bigtableRDD.writeRDD(erroneousMutationsRDD.rdd(), useTable, createRDDConf());
        fail("The connector should have thrown a " + Status.NOT_FOUND + " exception.");
      } catch (Exception e) {
        assertThat(
            ExceptionUtils.getStackTrace(e), containsString(Status.NOT_FOUND.getCode().toString()));
      }

    } finally {
      deleteBigtableTable(useTable, adminClient);
    }
  }

  private JavaRDD<Tuple2<String, Long>> createTestRDD(int numOfRows) {
    List<Tuple2<String, Long>> pairs = new ArrayList<>();
    for (int i = 0; i < numOfRows; i++) {
      pairs.add(new Tuple2<>("key" + i, (long) i));
    }
    return javaSparkContext.parallelize(pairs);
  }

  private JavaRDD<RowMutationEntry> convertTupleRDDToMutationsRDD(
      JavaRDD<Tuple2<String, Long>> tupleRDD) {
    return tupleRDD.map(
        tuple -> {
          String rowKey = tuple._1();
          Long value = tuple._2();
          return RowMutationEntry.create(rowKey)
              .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER, value.toString());
        });
  }

  private JavaRDD<RowMutationEntry> convertTupleRDDToErroneousMutationsRDD(
      JavaRDD<Tuple2<String, Long>> tupleRDD) {
    return tupleRDD.map(
        tuple -> {
          String rowKey = tuple._1();
          Long value = tuple._2();
          String columnFamily = (value == 0L) ? NONEXISTENT_COLUMN_FAMILY : COLUMN_FAMILY;
          return RowMutationEntry.create(rowKey)
              .setCell(columnFamily, COLUMN_QUALIFIER, value.toString());
        });
  }

  BigtableSparkConf createRDDConf() {
    return new BigtableSparkConfBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
  }

  void assertTupleRDDsEqual(
      JavaRDD<Tuple2<String, Long>> actualRDD, JavaRDD<Tuple2<String, Long>> expectedRDD) {
    List<Tuple2<String, Long>> actualList = new ArrayList<>(actualRDD.collect());
    List<Tuple2<String, Long>> expectedList = new ArrayList<>(expectedRDD.collect());

    actualList.sort((a, b) -> a._1.compareTo(b._1));
    expectedList.sort((a, b) -> a._1.compareTo(b._1));

    Assert.assertEquals(actualList, expectedList);
  }

  String testName() {
    return "rdd-integration";
  }
}
