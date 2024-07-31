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

package spark.bigtable.example;

import static org.apache.spark.sql.functions.callUDF;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import spark.bigtable.example.model.TestRow;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import java.util.Random;
import scala.collection.JavaConverters;
import scala.collection.Seq;


public class WordCountWrite {
  private static SparkSession spark;
  private static String projectId;
  private static String instanceId;
  private static String tableName;
  private static long initialRowCount = 1_000;
  private static int expansionFactor = 100;
  private static String createNewTable = "true";

  private static void parseArguments(String[] args) throws IllegalArgumentException {
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Arguments Bigtable project ID, instance ID, " + "and table name must be specified");
    }
    projectId = args[0];
    instanceId = args[1];
    tableName = args[2];
    if (args.length > 3) {
      initialRowCount = Long.parseLong(args[3]);
    }
    if (args.length > 4) {
      expansionFactor = Integer.parseInt(args[4]);
    }
    if (args.length > 5) {
      createNewTable = args[5];
    }
  }

  public static void main(String[] args) throws IllegalArgumentException {
    parseArguments(args);

    String catalog =
        "{"
            + "\"table\":{\"name\":\""
            + tableName
            + "\","
            + "\"tableCoder\":\"PrimitiveType\"},"
            + "\"rowkey\":\"wordCol\","
            + "\"columns\":{"
            + "\"wordCol\":"
            + "{\"cf\":\"rowkey\", \"col\":\"wordCol\", \"type\":\"string\"},"
            + "\"count\":"
            + "{\"cf\":\"cf\", \"col\":\"Count\", \"type\":\"int\"}"
            + "}}".replaceAll("\\s+", "");

    SparkConf conf = new SparkConf().setAppName("DistributedRandomData");
    JavaSparkContext sc = new JavaSparkContext(conf);
    spark = SparkSession.builder().getOrCreate();

    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField("wordCol", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("count", DataTypes.LongType, false));
    StructType schema = DataTypes.createStructType(fields);

    JavaRDD<Row> initialRDD = sc.parallelize(generateRandomRows(initialRowCount), sc.defaultParallelism())
        .map(row -> RowFactory.create(row.get(0), row.get(1)));

    JavaRDD<Row> expandedRDD = initialRDD.flatMap(row ->
        JavaConverters.asJavaIteratorConverter(
            JavaConverters.asScalaBufferConverter(expandRow(row, expansionFactor)).asScala().iterator()
        ).asJava()
    );

    Dataset<Row> dfToWrite = spark.createDataFrame(expandedRDD, schema);

//    long finalCount = dfToWrite.count();
//    System.out.println("Final row count: " + finalCount);
//    dfToWrite.show();

    writeDataframeToBigtable(dfToWrite, catalog, createNewTable);
    System.out.println("DataFrame was written to Bigtable.");
//    List<Row> first10Rows = dfToWrite.limit(10).collectAsList();
//    for (Row row : first10Rows) {
//      System.out.println(row);
//    }

    sc.stop();
  }

  // Helper to generate random rows locally
  private static List<List<Object>> generateRandomRows(long count) {
    List<List<Object>> rows = new ArrayList<>();
    Random random = new Random();
    for (long i = 0; i < count; i++) {
      List<Object> row = new ArrayList<>();
      row.add(random.nextLong());
      row.add(i);
      rows.add(row);
    }
    return rows;
  }

  private static List<Row> expandRow(Row row, int factor) {
    List<Row> expandedRows = new ArrayList<>();
    for (int i = 0; i < factor; i++) {
      expandedRows.add(RowFactory.create(
          String.format("Row%020d-%08d", row.getLong(0), i),
          row.getLong(1) * factor + i
      ));
    }
    return expandedRows;
  }

  private static Dataset<Row> createTestDataFrame() {
    ArrayList<TestRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(new TestRow(String.format("word%d", i), i, i / 1000.0));
    }
    return spark.createDataset(rows, Encoders.bean(TestRow.class)).toDF();
  }

  private static void writeDataframeToBigtable(
      Dataset<Row> dataframe, String catalog, String createNewTable) {
    dataframe
        .write()
        .format("bigtable")
        .option("catalog", catalog)
        .option("spark.bigtable.project.id", projectId)
        .option("spark.bigtable.instance.id", instanceId)
        .option("spark.bigtable.create.new.table", createNewTable)
        .save();
  }

  private static Dataset<Row> readDataframeFromBigtable(String catalog) {
    return spark
        .read()
        .format("bigtable")
        .option("catalog", catalog)
        .option("spark.bigtable.project.id", projectId)
        .option("spark.bigtable.instance.id", instanceId)
        .option("spark.bigtable.app_profile.id", "databoost-test-1")
        .load();
  }
}
