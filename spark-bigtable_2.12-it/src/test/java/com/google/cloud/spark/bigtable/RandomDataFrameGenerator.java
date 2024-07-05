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

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomDataFrameGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(RandomDataFrameGenerator.class);

  final String colFamilyName = "col_family";
  private Dataset<Row> df;
  private List<Row> dfRows;
  // We first generate a larger DataFrame and then remove duplicate row keys.
  private final int initialNumOfRows;
  private int numOfRows;
  private final int numOfCols;

  private String rowKeyCol;

  private SparkSession spark;
  private StructType schema;

  private Random random;

  RandomDataFrameGenerator(SparkSession spark, int minRows, int maxRows, int minCols, int maxCols) {
    this.spark = spark;
    random = new Random();

    this.initialNumOfRows = random.nextInt(maxRows - minRows + 1) + minRows;
    this.numOfCols = random.nextInt(maxCols - minCols + 1) + minCols;

    fillDataFrame();
  }

  String getRawSchemaString() {
    StringBuilder rawCatalog =
        new StringBuilder(
            "{\"table\":{\"name\":\"${tablename}\"," + "\"tableCoder\":\"PrimitiveType\"},");
    rawCatalog.append("\"rowkey\":\"").append(rowKeyCol).append("\",");
    rawCatalog.append("\"columns\":{");
    for (int i = 0; i < numOfCols; i++) {
      String colName = schema.apply(i).name();
      String colFamily = (colName.equals(rowKeyCol)) ? "rowkey" : colFamilyName;
      String type;
      DataType dataType = schema.apply(i).dataType();
      if (dataType.equals(DataTypes.BooleanType)) {
        type = "boolean";
      } else if (dataType.equals(DataTypes.ByteType)) {
        type = "byte";
      } else if (dataType.equals(DataTypes.ShortType)) {
        type = "short";
      } else if (dataType.equals(DataTypes.IntegerType)) {
        type = "int";
      } else if (dataType.equals(DataTypes.LongType)) {
        type = "long";
      } else if (dataType.equals(DataTypes.FloatType)) {
        type = "float";
      } else if (dataType.equals(DataTypes.DoubleType)) {
        type = "double";
      } else if (dataType.equals(DataTypes.TimestampType)) {
        type = "timestamp";
      } else if (dataType.equals(DataTypes.StringType)) {
        type = "string";
      } else if (dataType.equals(DataTypes.BinaryType)) {
        type = "binary";
      } else {
        throw new IllegalStateException("Unexpected column type " + dataType);
      }
      rawCatalog.append(
          "\""
              + colName
              + "\":{\"cf\":\""
              + colFamily
              + "\", \"col\":\""
              + colName
              + "\","
              + " \"type\":\""
              + type
              + "\"}");
      if (i != numOfCols - 1) {
        rawCatalog.append(",");
      }
    }
    rawCatalog.append("}}");
    return rawCatalog.toString();
  }

  Dataset<Row> getDf() {
    return df;
  }

  StructType getSchema() {
    return schema;
  }

  int getNumOfCols() {
    return numOfCols;
  }

  String getRowKeyCol() {
    return rowKeyCol;
  }

  private void fillDataFrame() {
    generateRandomColumns();
    List<Row> data = new ArrayList<>();
    for (int i = 1; i <= initialNumOfRows; i++) {
      List<Object> rowColumns = new ArrayList<>();
      for (int j = 0; j < numOfCols; j++) {
        rowColumns.add(generateRandomValue(schema.apply(j).dataType()));
      }
      data.add(RowFactory.create(rowColumns.toArray()));
    }

    // We first need to drop duplicate row key column values to avoid collision.
    df = spark.createDataFrame(data, schema).dropDuplicates(rowKeyCol);
    dfRows = df.collectAsList();
    numOfRows = dfRows.size();

    LOG.info(
        "Generated a random DataFrame with " + numOfRows + " rows and " + numOfCols + " columns.");
    LOG.info("Random DataFrame's schema = " + schema);
    df.show();
  }

  private void generateRandomColumns() {
    schema = new StructType();
    for (int i = 0; i < numOfCols; i++) {
      DataType dt = getRandomDataType();
      schema = schema.add("col-" + i, dt, true);
    }
    rowKeyCol = schema.apply(random.nextInt(numOfCols)).name();
  }

  private DataType getRandomDataType() {
    // We can add more types to this list as they become supported in the connector.
    DataType[] types = {
        DataTypes.BooleanType,
        DataTypes.ByteType,
        DataTypes.ShortType,
        DataTypes.IntegerType,
        DataTypes.LongType,
        DataTypes.FloatType,
        DataTypes.DoubleType,
        DataTypes.StringType,
        DataTypes.BinaryType
    };
    int dataTypeCode = random.nextInt(types.length);
    return types[dataTypeCode];
  }

  private Object generateRandomValue(DataType dataType) {
    if (dataType.equals(DataTypes.BooleanType)) {
      return random.nextBoolean();
    } else if (dataType.equals(DataTypes.ByteType)) {
      return (byte) random.nextInt(256);
    } else if (dataType.equals(DataTypes.ShortType)) {
      return (short) random.nextInt(65536);
    } else if (dataType.equals(DataTypes.IntegerType)) {
      return random.nextInt();
    } else if (dataType.equals(DataTypes.LongType)) {
      return random.nextLong();
    } else if (dataType.equals(DataTypes.FloatType)) {
      return random.nextFloat();
    } else if (dataType.equals(DataTypes.DoubleType)) {
      return random.nextDouble();
    } else if (dataType.equals(DataTypes.TimestampType)) {
      // Avoid overflow since the connector multiplies the timestamp by 1000 (microseconds).
      return new Timestamp(random.nextLong() / 2000);
    } else if (dataType.equals(DataTypes.StringType)) {
      return new String(generateRandomBytes(), StandardCharsets.UTF_8);
    } else if (dataType.equals(DataTypes.BinaryType)) {
      return generateRandomBytes();
    } else {
      throw new IllegalArgumentException("Unsupported column data type: " + dataType + ".");
    }
  }

  private byte[] generateRandomBytes() {
    int len = random.nextInt(20) + 1;
    byte[] randomBytes = new byte[len];
    random.nextBytes(randomBytes);
    return randomBytes;
  }

  Column generateRandomFilter(int maxDepth, double deeperProb) {
    // Since we are guarding against false negatives, we use OR filters more frequently to
    // avoid most filters having empty results.
    double andProb = 0.3;

    Column newFilter;
    if (maxDepth <= 1) {
      return generateSingleFilter();
    } else {
      double deeperRandom = Math.random();
      if (deeperRandom < deeperProb) {
        Column left = generateRandomFilter(maxDepth - 1, deeperProb * 0.6);
        Column right = generateRandomFilter(maxDepth - 1, deeperProb * 0.6);
        if (Math.random() < andProb) {
          newFilter = left.and(right);
        } else {
          newFilter = left.or(right);
        }
      } else {
        newFilter = generateSingleFilter();
      }
    }

    return newFilter;
  }

  Column generateSingleFilter() {
    // For an example filter (col == X), the probability that the value X will be randomly generated
    // instead of being picked from the column values.
    double newValueProb = 0.1;
    // The probability of adding a NOT filter to the generated filter.
    double negateProb = 0.2;

    int randomColNumber = random.nextInt(numOfCols);
    String colName = schema.apply(randomColNumber).name();

    Object value;
    if (Math.random() < newValueProb) {
      value = generateRandomValue(schema.apply(randomColNumber).dataType());
    } else {
      int randomRowNumber = random.nextInt(numOfRows);
      value = dfRows.get(randomRowNumber).get(randomColNumber);
    }

    Column filter;
    int randOperator = random.nextInt(6);
    switch (randOperator) {
      case 0:
        filter = functions.col(colName).$greater(value);
        break;
      case 1:
        filter = functions.col(colName).$greater$eq(value);
        break;
      case 2:
        filter = functions.col(colName).$less(value);
        break;
      case 3:
        filter = functions.col(colName).$less$eq(value);
        break;
      case 4:
        if (schema.apply(colName).dataType() == DataTypes.StringType && random.nextBoolean()) {
          String valueStr = value.toString();
          String prefix = valueStr.substring(0, random.nextInt(valueStr.length()));
          filter = functions.col(colName).startsWith(prefix);
        } else {
          filter = functions.col(colName).equalTo(value);
        }
        break;
      default:
        filter = functions.col(colName).notEqual(value);
        break;
    }

    if (Math.random() < negateProb) {
      filter = functions.not(filter);
    }
    return filter;
  }
}
