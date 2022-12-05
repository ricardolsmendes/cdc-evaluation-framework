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
    def test_read_transactions_should_return_data_frame_on_success(self, mock_read_csv):
        fake_data_frame = pd.DataFrame()
        mock_read_csv.return_value = fake_data_frame
        return_value = online_retail.CSVFilesReader.read_transactions('test.csv')
        self.assertTrue(fake_data_frame.equals(return_value))


class TransactionsDBManagerTest(unittest.TestCase):
    _DB_MANAGER_CLASS = f'{_ONLINE_RETAIL_MODULE}.TransactionsDBManager'

    def setUp(self):
        self._db_manager = online_retail.TransactionsDBManager(
            db_conn_string='test-db-conn')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self._db_manager.__dict__
        self.assertEqual('test-db-conn', attrs['_db_conn_string'])

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.sqlalchemy.delete')
    @mock.patch(f'{_DB_MANAGER_CLASS}.get_existing_table')
    @mock.patch(f'{_DB_MANAGER_CLASS}.create_db_connection')
    def test_delete_invoices_should_delete_by_unique_invoice_numbers(
            self, mock_create_db_connection, mock_get_existing_table, mock_delete):

        fake_data = {
            'Invoice': [489434, 489434, 489435, 489436, 489436, 489437],
            'StockCode': ['85048', '79323P', '22350', '48173C', '21755', '22143'],
            'Description': [
                '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 'PINK CHERRY LIGHTS', 'CAT BOWL',
                'DOOR MAT BLACK FLOCK', 'LOVE BUILDING BLOCK WORD',
                'CHRISTMAS CRAFT HEART DECORATIONS'
            ],
            'Quantity': [12, 12, 12, 18, 18, 6]
        }
        transactions = pd.DataFrame(fake_data)
        fake_conn = mock_create_db_connection.return_value

        self._db_manager.delete_invoices(transactions, 0)

        mock_get_existing_table.assert_called_once_with(fake_conn, 'transactions')
        self.assertEqual(mock_delete.call_count, 4)  # One for each invoice number
