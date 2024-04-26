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
import logging

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

class LoadTest(test_base.TestBase):
    total_rows = 1_000_000_000
    batch_size = 1_000_000
    batch_count = total_rows // batch_size

    def run_test(self):
        self.setup_basic_args()
        self.setup_spark()

        for batch_number in range(self.batch_count):
            data = [{'word': f'word{i:012d}', 'count': i}
                    for i in range(batch_number * self.batch_size,
                                (batch_number + 1) * self.batch_size)]
            input_df = self.spark.createDataFrame(data)
            create_new_table = 'true' if batch_number == 0 else 'false'
            self.write_dataframe(input_df, create_new_table)
            logging.info('Finished writing batch #' + str(batch_number))

        read_df = self.read_dataframe()
        assert read_df.count() == self.total_rows

LoadTest().run_test()
