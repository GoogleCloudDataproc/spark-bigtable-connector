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

import com.google.cloud.spark.bigtable.join.BigtableJoin;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import spark.bigtable.example.model.TestRow;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.expr;

public class JoinPushDown {
    private static SparkSession spark;
    private static String projectId;
    private static String instanceId;
    private static String tableName;
    private static String createNewTable = "true";

    private static void parseArguments(String[] args) throws IllegalArgumentException {
        if (args.length < 0) {
            throw new IllegalArgumentException(
              "Arguments Bigtable project ID, instance ID, " + "and table name must be specified");
        }
        projectId = args[0];
        instanceId = args[1];
        tableName = args[2];
        if (args.length > 3) {
            createNewTable = args[3];
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
            + "\"word\":"
            + "{\"cf\":\"rowkey\", \"col\":\"wordCol\", \"type\":\"string\"},"
            + "\"count\":"
            + "{\"cf\":\"example_family\", \"col\":\"countCol\", \"type\":\"long\"},"
            + "\"frequencyBinary\":"
            + "{\"cf\":\"example_family\", \"col\":\"frequencyCol\", \"type\":\"binary\"}"
            + "}}".replaceAll("\\s+", "");

        spark = SparkSession.builder().getOrCreate();
        spark.udf().register("doubleToBinary", new DoubleToBinaryUdf(), DataTypes.BinaryType);
        spark.udf().register("binaryToDouble", new BinaryToDoubleUdf(), DataTypes.DoubleType);

        Dataset<Row> dfWithDouble = createTestDataFrame();
        System.out.println("Created the DataFrame:");
        dfWithDouble.show();

        Dataset<Row> dfToWrite =
          dfWithDouble
            .withColumn(
              "frequencyBinary", callUDF("doubleToBinary", dfWithDouble.col("frequencyDouble")))
            .drop("frequencyDouble");

        writeDataframeToBigtable(dfToWrite, catalog, createNewTable);
        System.out.println("DataFrame was written to Bigtable.");

        //Bigtable join push down
        Dataset<Row> srcDf = dfToWrite.sample(100.0 / 1000.0).selectExpr("word", "count as src_count");

        Map<String, String> joinConfig = new HashMap<>();
        joinConfig.put("spark.bigtable.project.id", projectId);
        joinConfig.put("spark.bigtable.instance.id", instanceId);
        joinConfig.put("catalog", catalog);


        ArrayList<String> joinExpr3 = new ArrayList<>(2);
        joinExpr3.add("word");

        String[] joinExpr2 = new String[]{"word"};

        Column joinExpr1 = expr("a.word = bt.word");

        Dataset<Row> joinedDf1 = BigtableJoin.joinWithBigtable(srcDf.as("a"), joinConfig, "word", joinExpr1, "inner", "b", spark);
        Dataset<Row> joinedDf2 = BigtableJoin.joinWithBigtable(srcDf.as("a"), joinConfig, "word", joinExpr2, "inner", "b", spark);
        Dataset<Row> joinedDf3 = BigtableJoin.joinWithBigtable(srcDf.as("a"), joinConfig, "word", joinExpr3, "inner", "b", spark);

        System.out.println("Printing the Joined DataFrame:");
        joinedDf1.show();
        joinedDf2.show();
        joinedDf3.show();
    }

    private static Dataset<Row> createTestDataFrame() {
        ArrayList<TestRow> rows = new ArrayList<>();
        for (int i = 0; i < 999; i++) {
            rows.add(new TestRow(String.format("word%03d", i), i, i / 1000.0));
        }
        return spark.createDataset(rows, Encoders.bean(TestRow.class)).toDF();
    }

    private static class DoubleToBinaryUdf implements UDF1<Double, byte[]> {
        @Override
        public byte[] call(Double value) {
            if (value == null) {
                return null;
            }
            return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
        }
    }

    private static class BinaryToDoubleUdf implements UDF1<byte[], Double> {
        @Override
        public Double call(byte[] arr) {
            if (arr == null || arr.length == 0) {
                return null;
            }
            return ByteBuffer.wrap(arr).getDouble();
        }
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
}
