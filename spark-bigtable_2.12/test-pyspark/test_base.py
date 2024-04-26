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

from pyspark.sql import SparkSession
import argparse

class TestBase():
  PROJECT_ID_PROPERTY_NAME = 'bigtableProjectId'
  INSTANCE_ID_PROPERTY_NAME = 'bigtableInstanceId'
  TABLE_ID_PROPERTY_NAME = 'bigtableTableId'

  def setup_basic_args(self):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--' + self.PROJECT_ID_PROPERTY_NAME, help='Bigtable project ID.')
    parser.add_argument(
        '--' + self.INSTANCE_ID_PROPERTY_NAME, help='Bigtable instance ID.')
    parser.add_argument(
        '--' + self.TABLE_ID_PROPERTY_NAME, help='Bigtable table ID.')
    args = vars(parser.parse_args())  # Convert args from Namespace to dict.

    self.bigtable_project_id = args.get(self.PROJECT_ID_PROPERTY_NAME)
    self.bigtable_instance_id = args.get(self.INSTANCE_ID_PROPERTY_NAME)
    self.bigtable_table_id = args.get(self.TABLE_ID_PROPERTY_NAME)

    if not (self.bigtable_project_id and
            self.bigtable_instance_id and
            self.bigtable_table_id):
      raise ValueError(
        f'Bigtable project ID, instance ID, and table id should be specified '
        f'using --{self.PROJECT_ID_PROPERTY_NAME}=X, --{self.INSTANCE_ID_PROPERTY_NAME}=Y, '
        f'and --{self.TABLE_ID_PROPERTY_NAME}=Z, respectively.'
        )

    self.catalog = ''.join(("""{
      "table":{"namespace":"default", "name":" """ + self.bigtable_table_id + """
      ", "tableCoder":"PrimitiveType"},
      "rowkey":"word",
      "columns":{
      "word":{"cf":"rowkey", "col":"word", "type":"string"},
      "count":{"cf":"cf", "col":"Count", "type":"long"}
      }
      }""").split())

  def setup_spark(self):
    self.spark = SparkSession.builder \
          .appName('test') \
          .getOrCreate()

  def write_dataframe(self, df, create_new_table='true'):
    df.write \
        .format('bigtable') \
        .options(catalog=self.catalog) \
        .option('spark.bigtable.project.id', self.bigtable_project_id) \
        .option('spark.bigtable.instance.id', self.bigtable_instance_id) \
        .option('spark.bigtable.create.new.table', create_new_table) \
        .save()

  def read_dataframe(self):
    return self.spark.read \
        .format('bigtable') \
        .options(catalog=self.catalog) \
        .option('spark.bigtable.project.id', self.bigtable_project_id) \
        .option('spark.bigtable.instance.id', self.bigtable_instance_id) \
        .load()
