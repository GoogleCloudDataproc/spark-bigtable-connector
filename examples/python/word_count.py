# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import pyspark.sql.functions as F
import struct
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import BinaryType
from pyspark.sql.types import DoubleType

PROJECT_ID_PROPERTY_NAME = 'bigtableProjectId'
INSTANCE_ID_PROPERTY_NAME = 'bigtableInstanceId'
TABLE_NAME_PROPERTY_NAME = 'bigtableTableName'
CREATE_NEW_TABLE_PROPERTY_NAME = 'createNewTable'

parser = argparse.ArgumentParser()
parser.add_argument(
  '--' + PROJECT_ID_PROPERTY_NAME, help='Bigtable project ID.')
parser.add_argument(
  '--' + INSTANCE_ID_PROPERTY_NAME, help='Bigtable instance ID.')
parser.add_argument(
  '--' + TABLE_NAME_PROPERTY_NAME, help='Bigtable table name.')
parser.add_argument(
  '--' + CREATE_NEW_TABLE_PROPERTY_NAME,
  default='true',
  help='Whether to create a new Bigtable table.')
args = vars(parser.parse_args())  # Convert args from Namespace to dict.

bigtable_project_id = args.get(PROJECT_ID_PROPERTY_NAME)
bigtable_instance_id = args.get(INSTANCE_ID_PROPERTY_NAME)
bigtable_table_name = args.get(TABLE_NAME_PROPERTY_NAME)
create_new_table = args.get(CREATE_NEW_TABLE_PROPERTY_NAME)

if not (bigtable_project_id and
        bigtable_instance_id and
        bigtable_table_name):
  raise ValueError(
    f'Bigtable project ID, instance ID, and table id should be specified '
    f'using --{PROJECT_ID_PROPERTY_NAME}=X, --{INSTANCE_ID_PROPERTY_NAME}=Y, '
    f'and --{TABLE_NAME_PROPERTY_NAME}=Z, respectively.'
  )


def double_to_binary(double_value):
  if double_value is None:
    return None
  return struct.pack('>d', double_value)


def binary_to_double(byte_array):
  if not byte_array:
    return None
  return struct.unpack('>d', byte_array)[0]


double_to_binary_udf = F.udf(double_to_binary, BinaryType())
binary_to_double_udf = F.udf(binary_to_double, DoubleType())

spark = SparkSession.builder.getOrCreate()

catalog = ''.join(("""{
      "table":{"namespace":"default", "name":" """ + bigtable_table_name + """
       ", "tableCoder":"PrimitiveType"},
      "rowkey":"wordCol",
      "columns":{
        "word":{"cf":"rowkey", "col":"wordCol", "type":"string"},
        "count":{"cf":"example_family", "col":"countCol", "type":"long"},
        "frequency_binary":{"cf":"example_family", "col":"frequencyCol", "type":"binary"}
      }
      }""").split())

data = [{'word': f'word{i}', 'count': i, 'frequency_double': i / 1000.0} for i in range(10)]
df_with_double = spark.createDataFrame(data)
print('Created the DataFrame:')
df_with_double.show()
df_to_write = (df_with_double
               .withColumn("frequency_binary", double_to_binary_udf(F.col("frequency_double")))
               .drop("frequency_double"))

df_to_write.write \
  .format('bigtable') \
  .options(catalog=catalog) \
  .option('spark.bigtable.project.id', bigtable_project_id) \
  .option('spark.bigtable.instance.id', bigtable_instance_id) \
  .option('spark.bigtable.create.new.table', create_new_table) \
  .save()
print('DataFrame was written to Bigtable.')

readDf = spark.read \
  .format('bigtable') \
  .option('spark.bigtable.project.id', bigtable_project_id) \
  .option('spark.bigtable.instance.id', bigtable_instance_id) \
  .options(catalog=catalog) \
  .load()

readDfWithDouble = (readDf
                    .withColumn("frequency_double", binary_to_double_udf(F.col("frequency_binary")))
                    .drop("frequency_binary"))

print('Reading the DataFrame from Bigtable:')
readDfWithDouble.show()

## join push down
bigtable_join_class = spark._jvm.com.google.cloud.spark.bigtable.join.BigtableJoin
config_map = spark._jvm.java.util.HashMap()
config_map.put("spark.bigtable.project.id", bigtable_project_id)
config_map.put("spark.bigtable.instance.id", bigtable_instance_id)
config_map.put("catalog", catalog)
config_map.put("join.type", "inner")
config_map.put("columns.required", "word,count")
config_map.put("partition.count", "10")
config_map.put("batch.rowKeySize", "100")
config_map.put("alias.name", "bt")
row_key = "word"
join_expr = "word"
result_df = bigtable_join_class.joinWithBigtable(readDf._jdf, config_map, row_key, join_expr, spark._jsparkSession)
pyspark_result_df = DataFrame(result_df, spark)

print("\nPrinting joined dataframe")
pyspark_result_df.show()
