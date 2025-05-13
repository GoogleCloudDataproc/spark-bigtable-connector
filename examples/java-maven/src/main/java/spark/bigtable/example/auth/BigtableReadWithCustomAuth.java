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

package spark.bigtable.example.auth;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.bigtable.example.Util;

public class BigtableReadWithCustomAuth {

    public static void main(String[] args) throws IOException {
        String[] parsedArgs = parseArgs(args);
        String projectId = parsedArgs[0];
        String instanceId = parsedArgs[1];
        String tableName = parsedArgs[2];
        String createNewTable = parsedArgs[3];

        SparkSession spark =
                SparkSession.builder()
                        .appName("BigtableReadWithCustomAuth")
                        .getOrCreate();

        CustomCredentialProvider credentialsProvider = new CustomCredentialProvider();

        Util.createExampleBigtable(spark, projectId, instanceId, tableName, createNewTable);

        try {
            Dataset<Row> readDf =
                    spark
                            .read()
                            .format("bigtable")
                            .option("catalog", Util.getCatalog(tableName))
                            .option("spark.bigtable.project.id", projectId)
                            .option("spark.bigtable.instance.id", instanceId)
                            .option("spark.bigtable.auth.credentials_provider", credentialsProvider.getClass().getName())
                            .load();

            System.out.println("Reading data from Bigtable...");
            readDf.show(50);

        } catch (Exception e) {
            System.out.println("Error reading/writing data: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

    private static String[] parseArgs(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException(
                    "Missing command-line arguments. Required arguments: "
                            + "SPARK_BIGTABLE_PROJECT_ID SPARK_BIGTABLE_INSTANCE_ID SPARK_BIGTABLE_TABLE_NAME");
        }

        String projectId = args[0];
        String instanceId = args[1];
        String tableName = args[2];
        String createNewTable = args.length > 3 ? args[3] : "false";

        return new String[]{projectId, instanceId, tableName, createNewTable};
    }
}