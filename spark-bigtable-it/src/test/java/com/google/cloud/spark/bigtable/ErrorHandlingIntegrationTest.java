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

import com.google.cloud.spark.bigtable.fakeserver.FakeGenericDataService;
import com.google.cloud.spark.bigtable.fakeserver.FakeServerBuilder;
import com.google.cloud.spark.bigtable.fakeserver.FakeTableAdminService;
import com.google.cloud.spark.bigtable.model.TestRow;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.SampleRowKeysRequest;
import io.grpc.Server;
import io.grpc.Status;
import com.google.errorprone.annotations.Keep;
import java.util.ArrayList;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnitParamsRunner.class)
public class ErrorHandlingIntegrationTest extends AbstractTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlingIntegrationTest.class);

  private String emulatorPort;

  private Server server;
  private FakeGenericDataService fakeGenericDataService;
  private FakeTableAdminService fakeAdminService;

  private String FAKE_ERROR_MESSAGE = "Fake error message";

  @BeforeClass
  public static void initialBeforeClassSetup() throws Exception {
    spark = createSparkSession();
  }

  @Before
  public void initialSetup() throws Exception {
    fakeGenericDataService = new FakeGenericDataService();
    fakeAdminService = new FakeTableAdminService();
    server =
        new FakeServerBuilder()
            .addService(fakeGenericDataService)
            .addService(fakeAdminService)
            .start();
    LOG.info("Bigtable mock server started on port " + server.getPort());
    emulatorPort = Integer.toString(server.getPort());

    setBigtableProperties();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stopSparkSession(spark);
  }

  @After
  public void tearDownServer() throws InterruptedException {
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @Keep
  private static Status[] fetchStatuses() {
    return new Status[] {
      Status.INTERNAL,
      Status.UNAUTHENTICATED,
      Status.NOT_FOUND,
      Status.FAILED_PRECONDITION,
      Status.INVALID_ARGUMENT,
      Status.PERMISSION_DENIED
    };
  }

  @Keep
  private static Status[] fetchCreateTableStatuses() {
    return new Status[] {
      Status.INTERNAL,
      Status.UNAUTHENTICATED,
      Status.NOT_FOUND,
      Status.FAILED_PRECONDITION,
      Status.INVALID_ARGUMENT,
      Status.PERMISSION_DENIED,
      Status.UNAVAILABLE,
      Status.ALREADY_EXISTS
    };
  }

  @Test
  @Parameters(method = "fetchStatuses")
  public void testReadRowsErrors(Status status) throws Exception {
    String useTable = generateTableId();
    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

    createEmptyTestDataFrame();
    LOG.info("Empty DataFrame Created.");

    // This test tries to read two partitions, but only one of them would fail.
    fakeGenericDataService.setNumOfResponses(2);
    fakeGenericDataService.addError(
        ReadRowsRequest.newBuilder().build(), status.withDescription(FAKE_ERROR_MESSAGE));
    Dataset<Row> readDf =
        readDataframeFromBigtable(spark, catalog, withReaderEmulatorPort(emulatorPort));
    try {
      readDf.show();
      fail("The connector should have thrown an " + status + " exception.");
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString(status.getCode().toString()));
      assertThat(e.getMessage(), containsString(FAKE_ERROR_MESSAGE));
    }
  }

  @Test
  @Parameters(method = "fetchStatuses")
  public void testSampleRowKeysErrors(Status status) throws Exception {
    String useTable = generateTableId();
    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

    createEmptyTestDataFrame();
    LOG.info("Empty DataFrame Created.");

    fakeGenericDataService.addError(
        SampleRowKeysRequest.newBuilder().build(), status.withDescription(FAKE_ERROR_MESSAGE));
    Dataset<Row> readDf =
        readDataframeFromBigtable(spark, catalog, withReaderEmulatorPort(emulatorPort));
    try {
      readDf.show();
      fail("The connector should have thrown an " + status + " exception.");
    } catch (Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), containsString(status.getCode().toString()));
      assertThat(ExceptionUtils.getStackTrace(e), containsString(FAKE_ERROR_MESSAGE));
    }
  }

  @Test
  @Parameters(method = "fetchStatuses")
  public void testMutateRowsErrors(Status status) throws Exception {
    String useTable = generateTableId();
    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

    createEmptyTestDataFrame();
    LOG.info("Empty DataFrame Created.");

    // Only the first out of the three batch would fail.
    fakeGenericDataService.addError(
        MutateRowsRequest.newBuilder().build(), status.withDescription("Fake internal message."));
    try {
      writeDataframeToBigtable(
          createTestDataFrame(250),
          catalog,
          false,
          withWriterEmulatorPort(emulatorPort),
          withWriterBatchMutateSize("100"));
      fail("The connector should have thrown an " + status + " exception.");
    } catch (Exception e) {
      LOG.info(ExceptionUtils.getStackTrace(e));
      // TODO : Fix test. Currently the error message is "Job aborted.".
      // https://github.com/apache/spark/blob/14b00cfc6c2e477067fc9e7937e34b2aa53df1eb/core/src/main/scala/org/apache/spark/internal/io/SparkHadoopWriter.scala#L106
      // assertThat(e.getMessage(), containsString(status.getCode().toString()));
      // assertThat(e.getMessage(), containsString("Fake internal message."));
    }
  }

  @Test
  @Parameters(method = "fetchCreateTableStatuses")
  public void testMutateRowsErrors_AdminErrors_CreateTable(Status status) throws Exception {
    String useTable = generateTableId();
    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

    createEmptyTestDataFrame();
    LOG.info("Empty DataFrame Created.");

    fakeAdminService.addError(
        CreateTableRequest.newBuilder().build(), status.withDescription("Fake internal message."));
    try {
      writeDataframeToBigtable(
          createTestDataFrame(1), catalog, true, withWriterEmulatorPort(emulatorPort));
      fail("The connector should have thrown an " + status + " exception.");
    } catch (Exception e) {
      assertThat(ExceptionUtils.getStackTrace(e), containsString(status.getCode().toString()));
      assertThat(ExceptionUtils.getStackTrace(e), containsString("Fake internal message."));
    }
  }

  @Test
  @Parameters({"1, false", "2, false", "3, true"})
  public void testReadRowsUnavailableErrors(int errorCount, boolean shouldFail) throws Exception {
    Status status = Status.UNAVAILABLE;
    String useTable = generateTableId();
    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

    createEmptyTestDataFrame();
    LOG.info("Empty DataFrame Created.");

    for (int i = 0; i < errorCount; i++) {
      fakeGenericDataService.addError(
          ReadRowsRequest.newBuilder().build(), status.withDescription("Fake internal message."));
    }

    // We test retrying the request three times before failing.
    Dataset<Row> readDf =
        readDataframeFromBigtable(
            spark, catalog, withReaderEmulatorPort(emulatorPort), withReaderMaxRetries("3"));
    try {
      readDf.show();
      if (shouldFail) {
        fail("The connector should have thrown an " + status + " exception.");
      }
    } catch (Exception e) {
      if (!shouldFail) {
        fail("The connector should not have thrown an " + status + " exception.");
      }
      assertThat(e.getMessage(), containsString(status.getCode().toString()));
      assertThat(e.getMessage(), containsString("Fake internal message."));
    }
  }

  @Test
  public void testSampleRowKeysUnavailableErrors() throws Exception {
    Status status = Status.UNAVAILABLE;
    String useTable = generateTableId();
    String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

    createEmptyTestDataFrame();
    LOG.info("Empty DataFrame Created.");

    fakeGenericDataService.addError(
        SampleRowKeysRequest.newBuilder().build(),
        status.withDescription("Fake internal message."));
    Dataset<Row> readDf =
        readDataframeFromBigtable(spark, catalog, withReaderEmulatorPort(emulatorPort));
    try {
      readDf.show();
    } catch (Exception e) {
      fail("The connector should not have thrown an " + status + " exception.");
    }
  }

  // TODO: currently fails, fix by not propagating the UNAVAILABLE error.
  // @Test
  // public void testMutateRowsUnavailableErrors() throws Exception {
  //   Status status = Status.UNAVAILABLE;
  //   String useTable = generateTableId();
  //   String catalog = parameterizeCatalog(rawBasicCatalog, useTable);

  //   createEmptyTestDataFrame();
  //   LOG.info("Empty DataFrame Created.");

  //   fakeGenericDataService.addError(
  //       MutateRowsRequest.newBuilder().build(), status.withDescription("Fake internal
  // message."));
  //   try {
  //     writeDataframeToBigtable(
  //         createTestDataFrame(1),
  //         catalog,
  //         false,
  //         withWriterEmulatorPort(emulatorPort));
  //   } catch (Exception e) {
  //     fail("The connector should not have thrown an " + status + " exception.");
  //   }
  // }

  // Since we are using a fake Bigtable server, an empty DataFrame is enough.
  private Dataset<Row> createEmptyTestDataFrame() {
    ArrayList<TestRow> rows = new ArrayList<>();
    Dataset<Row> df = spark.createDataset(rows, Encoders.bean(TestRow.class)).toDF();
    return df;
  }

  String testName() {
    return "error-handling";
  }
}
