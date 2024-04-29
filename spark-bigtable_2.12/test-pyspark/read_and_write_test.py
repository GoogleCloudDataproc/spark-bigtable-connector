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

import test_base


class ReadAndWriteTest(test_base.TestBase):
  def run_test(self):
    self.setup_basic_args()
    self.setup_spark()

    data = [{'word': f'word{i}', 'count': i} for i in range(10)]
    input_df = self.spark.createDataFrame(data)
    self.write_dataframe(input_df)

    read_df = self.read_dataframe()

    assert read_df.count() == 10
    assert len(read_df.columns) == 2
    assert sorted(read_df.columns) == sorted(['word', 'count'])

    filtered_df = read_df.filter(read_df.word == 'word4')
    assert filtered_df.count() == 1
    result_row = filtered_df.collect()[0]
    assert result_row['word'] == 'word4'
    assert result_row['count'] == 4


ReadAndWriteTest().run_test()
