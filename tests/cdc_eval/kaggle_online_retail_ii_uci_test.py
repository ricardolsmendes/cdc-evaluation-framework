# Copyright 2022 Ricardo Mendes
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

import unittest
from unittest import mock

import pandas as pd

from cdc_eval import kaggle_online_retail_ii_uci as online_retail

_ONLINE_RETAIL_MODULE = 'cdc_eval.kaggle_online_retail_ii_uci'


class CSVFilesReaderTest(unittest.TestCase):

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.read_csv')
    def test_read_transactions_should_read_csv(self, mock_read_csv):
        online_retail.CSVFilesReader.read_transactions('test.csv')
        mock_read_csv.assert_called_once_with('test.csv')

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.read_csv')
    def test_read_transactions_should_return_data_frame(self, mock_read_csv):
        fake_data_frame = pd.DataFrame()
        mock_read_csv.return_value = fake_data_frame
        return_value = online_retail.CSVFilesReader.read_transactions('test.csv')
        self.assertTrue(fake_data_frame.equals(return_value))
