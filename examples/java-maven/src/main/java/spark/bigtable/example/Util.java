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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Util {

    public static String getCatalog(String tableName) {
        return String.format(
                "{"
                        + "\"table\":{\"namespace\":\"default\", \"name\":\"%s\", \"tableCoder\":\"PrimitiveType\"},"
                        + "\"rowkey\":\"wordCol\","
                        + "\"columns\":{"
                        + "  \"word\":{\"cf\":\"rowkey\", \"col\":\"wordCol\", \"type\":\"string\"},"
                        + "  \"count\":{\"cf\":\"example_family\", \"col\":\"countCol\", \"type\":\"long\"},"
                        + "  \"frequency_binary\":{\"cf\":\"example_family\", \"col\":\"frequencyCol\", \"type\":\"binary\"}"
                        + "}"
                        + "}",
                tableName);
    }

    public static void createExampleBigtable(
            SparkSession spark,
            String projectId,
            String instanceId,
            String tableName,
            String createNewTable) {

        UserDefinedFunction doubleToBinaryUdf =
                udf((Double value) -> doubleToBinary(value), DataTypes.BinaryType);

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            data.add(RowFactory.create("word" + i, i, i / 1000.0));
        }

        StructType schema =
                DataTypes.createStructType(
                        new StructField[]{
                                DataTypes.createStructField("word", DataTypes.StringType, false),
                                DataTypes.createStructField("count", DataTypes.IntegerType, false),
                                DataTypes.createStructField("frequency_double", DataTypes.DoubleType, false)
                        });

        JavaRDD<Row> rdd = JavaSparkContext.fromSparkContext(spark.sparkContext()).parallelize(data);
        Dataset<Row> dfWithDouble = spark.createDataFrame(rdd, schema);

        System.out.println("Created the DataFrame:");
        dfWithDouble.show();

        Dataset<Row> dfToWrite =
                dfWithDouble
                        .withColumn("frequency_binary", doubleToBinaryUdf.apply(col("frequency_double")))
                        .drop("frequency_double");

        dfToWrite
                .write()
                .format("bigtable")
                .option("catalog", getCatalog(tableName))
                .option("spark.bigtable.project.id", projectId)
                .option("spark.bigtable.instance.id", instanceId)
                .option("spark.bigtable.create.new.table", createNewTable)
                .save();

        System.out.println("DataFrame was written to BigTable.");
    }

    private static byte[] doubleToBinary(Double value) {
        if (value == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        buffer.putDouble(value);
        return buffer.array();
    }
}