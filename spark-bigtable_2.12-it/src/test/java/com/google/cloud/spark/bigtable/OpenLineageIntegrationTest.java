package com.google.cloud.spark.bigtable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenLineageIntegrationTest extends AbstractTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(OpenLineageIntegrationTest.class);
  private static BigtableTableAdminClient adminClient;
  private static File lineageFile;

  @BeforeClass
  public static void initialSetup() throws Exception {
    System.out.println("*debugging* lineage test");
    spark = createSparkSessionWithOL();
    setBigtableProperties();

    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(projectId)
            .setInstanceId(instanceId)
            .build();
    adminClient = BigtableTableAdminClient.create(adminSettings);
  }

  @AfterClass
  public static void cleanup() {
    adminClient.close();
  }

  @Override
  String testName() {
    return "integration";
  }

  @Test
  public void testOpenLineageEvents() throws Exception {
    String inputTable = generateTableId();
    createBigtableTable(inputTable, adminClient);

    String outputTable = generateTableId();
    createBigtableTable(outputTable, adminClient);

    try {
      Dataset<Row> testDataFrame = createTestDataFrame(10);
      String catalog = parameterizeCatalog(rawBasicCatalog, inputTable);
      writeDataframeToBigtable(testDataFrame, catalog, false);

      Dataset<Row> readDf = readDataframeFromBigtable(spark, catalog);
      LOG.info("DataFrame was read from Bigtable.");

      readDf.registerTempTable("tempTable");

      Dataset<Row> outputDf =
          spark.sql("SELECT stringCol AS someCol, stringCol2 AS someCol2 FROM tempTable");

      String outputCatalogTemplate =
          "{\"table\":{\"name\":\"${tablename}\","
              + "\"tableCoder\":\"PrimitiveType\"},\"rowkey\":\"someCol\","
              + "\"columns\":{\"someCol\":{\"cf\":\"rowkey\", \"col\":\"someCol\","
              + " \"type\":\"string\"},\"someCol2\":{\"cf\":\"col_family1\","
              + " \"col\":\"someCol2\", \"type\":\"string\"}}}";

      String outputCatalog = parameterizeCatalog(outputCatalogTemplate, outputTable);
      writeDataframeToBigtable(outputDf, outputCatalog, false);

      // Ensure OpenLineage events have sufficient time to propagate to avoid incomplete or missing event data.
      Dataset<Row> outputReadDf = readDataframeFromBigtable(spark, outputCatalog);
      assertDataFramesEqual(outputReadDf, outputDf);

      List<JsonObject> jsonObjects = parseEventLog(lineageFile);
      assertThat(jsonObjects.isEmpty(), is(false));

      jsonObjects.forEach(
          jsonObject -> {
            JsonObject input = jsonObject.getAsJsonArray("inputs").get(0).getAsJsonObject();
            assertThat(
                input.get("namespace").getAsString(),
                is(String.format("bigtable://%s/%s", projectId, instanceId)));
            assertThat(input.get("name").getAsString(), is(inputTable));
            JsonObject output = jsonObject.getAsJsonArray("outputs").get(0).getAsJsonObject();
            assertThat(
                output.get("namespace").getAsString(),
                is(String.format("bigtable://%s/%s", projectId, instanceId)));
            assertThat(output.get("name").getAsString(), is(outputTable));
          });
    } finally {
      deleteBigtableTable(inputTable, adminClient);
      deleteBigtableTable(outputTable, adminClient);
    }
  }

  private static SparkSession createSparkSessionWithOL() throws IOException {
    lineageFile = File.createTempFile("openlineage_test_" + System.nanoTime(), ".log");
    lineageFile.deleteOnExit();

    spark =
        SparkSession.builder()
            .master("local")
            .appName("openlineage_test_bigtable_connector")
            .config("spark.ui.enabled", "false")
            .config("spark.default.parallelism", 20)
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .config("spark.openlineage.transport.type", "file")
            .config("spark.openlineage.transport.location", lineageFile.getAbsolutePath())
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    return spark;
  }

  private List<JsonObject> parseEventLog(File file) throws Exception {
    List<JsonObject> eventList;
    System.out.println("*debugging* in parseEvent");
    try (Scanner scanner = new Scanner(file)) {
      eventList = new ArrayList<>();
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        System.out.println("*debugging* line = " + line);
        JsonObject event = JsonParser.parseString(line).getAsJsonObject();
        if (!event.getAsJsonArray("inputs").isEmpty()
            && !event.getAsJsonArray("outputs").isEmpty()) {
          eventList.add(event);
        }
      }
    }
    return eventList;
  }
}
