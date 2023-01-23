# Copyright 2023 Ricardo Mendes
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
import sqlalchemy

from cdc_eval import kaggle_online_retail_ii_uci as online_retail

_ONLINE_RETAIL_MODULE = 'cdc_eval.kaggle_online_retail_ii_uci'


class CSVFilesReaderTest(unittest.TestCase):

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.read_csv')
    def test_read_transactions_should_read_csv(self, mock_read_csv):
        online_retail.CSVFilesReader.read_transactions('test.csv')
        mock_read_csv.assert_called_once_with('test.csv')

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.read_csv')
    def test_read_transactions_should_return_data_frame_on_success(self, mock_read_csv):
        data_frame = pd.DataFrame()
        mock_read_csv.return_value = data_frame
        transactions = online_retail.CSVFilesReader.read_transactions('test.csv')
        self.assertTrue(data_frame.equals(transactions))


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
    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.sqlalchemy.create_engine')
    def test_delete_invoices_should_delete_by_unique_invoice_numbers(
            self, mock_create_engine, mock_get_existing_table, mock_delete):

        transactions = pd.DataFrame({
            'Invoice': [489434, 489434, 489435, 489436, 489436, 489437],
            'StockCode': ['85048', '79323P', '22350', '48173C', '21755', '22143'],
            'Description': [
                '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 'PINK CHERRY LIGHTS', 'CAT BOWL',
                'DOOR MAT BLACK FLOCK', 'LOVE BUILDING BLOCK WORD',
                'CHRISTMAS CRAFT HEART DECORATIONS'
            ],
            'Quantity': [12, 12, 12, 18, 18, 6]
        })
        mock_conn = mock_create_engine.return_value

        self._db_manager.delete_invoices(transactions, 0)

        mock_create_engine.assert_called_once_with('test-db-conn')
        mock_get_existing_table.assert_called_once_with(mock_conn, 'transactions')
        self.assertEqual(mock_delete.call_count, 4)  # One call for each invoice number

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.io.sql.to_sql')
    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.sqlalchemy.create_engine')
    def test_insert_invoices_should_insert_item_batches_grouped_by_invoice(
            self, mock_create_engine, mock_to_sql):

        transactions = pd.DataFrame({
            'Invoice': [489434, 489434, 489435, 489436, 489436, 489437],
            'StockCode': ['85048', '79323P', '22350', '48173C', '21755', '22143'],
            'Description': [
                '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 'PINK CHERRY LIGHTS', 'CAT BOWL',
                'DOOR MAT BLACK FLOCK', 'LOVE BUILDING BLOCK WORD',
                'CHRISTMAS CRAFT HEART DECORATIONS'
            ],
            'Quantity': [12, 12, 12, 18, 18, 6]
        })

        self._db_manager.insert_invoices(transactions, 0)

        mock_create_engine.assert_called_once_with('test-db-conn')
        self.assertEqual(mock_to_sql.call_count, 4)  # One call for each invoice

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.sqlalchemy.MetaData')
    def test_get_existing_table_should_return_table_if_exists(self, mock_metadata):
        mock_conn = mock.MagicMock()
        tables = {'transactions': sqlalchemy.Table()}
        metadata = mock_metadata.return_value
        metadata.tables = tables

        table = self._db_manager.get_existing_table(mock_conn, 'transactions')

        self.assertEqual(tables['transactions'], table)
        metadata.reflect.assert_called_once_with(bind=mock_conn)

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.sqlalchemy.MetaData')
    def test_get_existing_table_should_return_none_if_not_exists(self, mock_metadata):
        mock_conn = mock.MagicMock()
        tables = {'transactions': sqlalchemy.Table()}
        metadata = mock_metadata.return_value
        metadata.tables = tables

        table = self._db_manager.get_existing_table(mock_conn, 'invoices')

        self.assertIsNone(table)
        metadata.reflect.assert_called_once_with(bind=mock_conn)


class PandasHelperTest(unittest.TestCase):
    _PANDAS_HELPER_CLASS = f'{_ONLINE_RETAIL_MODULE}.PandasHelper'

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.core.series.Series.unique')
    def test_get_unique_values_should_return_dataframe_with_unique_values_for_column(
            self, mock_unique):

        column = 'Invoice'

        df = pd.DataFrame({
            'Invoice': [489434, 489434, 489435, 489436, 489436, 489437],
            'StockCode': ['85048', '79323P', '22350', '48173C', '21755', '22143'],
            'Description': [
                '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 'PINK CHERRY LIGHTS', 'CAT BOWL',
                'DOOR MAT BLACK FLOCK', 'LOVE BUILDING BLOCK WORD',
                'CHRISTMAS CRAFT HEART DECORATIONS'
            ],
            'Quantity': [12, 12, 12, 18, 18, 6]
        })
        mock_unique.return_value = [489434, 489435, 489436, 489437]

        unique_values = online_retail.PandasHelper.get_unique_values(df, column)

        expected_df = pd.DataFrame({column: [489434, 489435, 489436, 489437]})
        pd.testing.assert_frame_equal(expected_df, unique_values)
        mock_unique.asset_called_once()

    @mock.patch(f'{_PANDAS_HELPER_CLASS}.select_random_items')
    @mock.patch(f'{_PANDAS_HELPER_CLASS}.get_unique_values')
    def test_select_random_subsets_should_return_dataframe_of_specified_size(
            self, mock_get_unique_values, mock_select_random_items):

        id_column = 'Invoice'

        df = pd.DataFrame({
            'Invoice': [489434, 489434, 489435, 489436, 489436, 489437],
            'StockCode': ['85048', '79323P', '22350', '48173C', '21755', '22143'],
            'Description': [
                '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 'PINK CHERRY LIGHTS', 'CAT BOWL',
                'DOOR MAT BLACK FLOCK', 'LOVE BUILDING BLOCK WORD',
                'CHRISTMAS CRAFT HEART DECORATIONS'
            ],
            'Quantity': [12, 12, 12, 18, 18, 6]
        })

        unique_values = pd.DataFrame({id_column: [489434, 489435, 489436, 489437]})
        mock_get_unique_values.return_value = unique_values

        random_items = pd.DataFrame({id_column: [489434, 489436]})
        mock_select_random_items.return_value = random_items

        subsets = online_retail.PandasHelper.select_random_subsets(df, id_column, 2)

        expected_df = pd.DataFrame({
            'Invoice': [489434, 489434, 489436, 489436],
            'StockCode': ['85048', '79323P', '48173C', '21755'],
            'Description': [
                '15CM CHRISTMAS GLASS BALL 20 LIGHTS', 'PINK CHERRY LIGHTS',
                'DOOR MAT BLACK FLOCK', 'LOVE BUILDING BLOCK WORD'
            ],
            'Quantity': [12, 12, 18, 18]
        })
        pd.testing.assert_frame_equal(expected_df, subsets)
        mock_get_unique_values.asset_called_once_with(df, 'Invoice')
        mock_select_random_items.asset_called_once_with(unique_values, 2)

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.pd.DataFrame.sample')
    def test_select_random_items_should_return_dataframe_of_specified_size(
            self, mock_sample):

        df = pd.DataFrame({'Invoice': [489434, 489435, 489436, 489437]})
        df_sample_return = pd.DataFrame({'Invoice': [489435, 489437]})
        mock_sample.return_value = df_sample_return

        random_items = online_retail.PandasHelper.select_random_items(df, 2)

        pd.testing.assert_frame_equal(df_sample_return, random_items)
        mock_sample.asset_called_once_with(2)


class RunnerTest(unittest.TestCase):
    _CSV_READER_CLASS = f'{_ONLINE_RETAIL_MODULE}.CSVFilesReader'
    _PANDAS_HELPER_CLASS = f'{_ONLINE_RETAIL_MODULE}.PandasHelper'

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.TransactionsDBManager')
    @mock.patch(f'{_PANDAS_HELPER_CLASS}.select_random_subsets')
    @mock.patch(f'{_CSV_READER_CLASS}.read_transactions')
    def test_run_should_read_all_transactions_from_file_and_insert_into_db(
            self, mock_read_transactions, mock_select_random_subsets, mock_db_manager):

        mock_conn = mock.MagicMock()
        transactions_df = pd.DataFrame()
        mock_read_transactions.return_value = transactions_df

        online_retail.Runner.run('test.csv', 0, mock_conn, 0, 'insert')

        mock_read_transactions.assert_called_once_with('test.csv')
        mock_select_random_subsets.assert_not_called()
        mock_db_manager.assert_called_once_with(mock_conn)
        mock_db_manager.return_value.delete_invoices.assert_not_called()
        mock_db_manager.return_value.insert_invoices.assert_called_once_with(
            transactions=transactions_df, operation_delay=0)

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.TransactionsDBManager', mock.MagicMock())
    @mock.patch(f'{_PANDAS_HELPER_CLASS}.select_random_subsets')
    @mock.patch(f'{_CSV_READER_CLASS}.read_transactions')
    def test_run_should_optionally_read_n_random_transactions_from_file(
            self, mock_read_transactions, mock_select_random_subsets):

        mock_conn = mock.MagicMock()
        transactions_df = pd.DataFrame()
        mock_read_transactions.return_value = transactions_df
        random_transactions_df = pd.DataFrame()
        mock_select_random_subsets.return_value = random_transactions_df

        online_retail.Runner.run('test.csv', 1000, mock_conn, 0, 'insert')

        mock_read_transactions.assert_called_once_with('test.csv')
        mock_select_random_subsets.assert_called_once_with(transactions_df, 'Invoice',
                                                           1000)

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.TransactionsDBManager')
    @mock.patch(f'{_CSV_READER_CLASS}.read_transactions', lambda *args: None)
    def test_run_should_insert_into_db_by_default(self, mock_db_manager):

        online_retail.Runner.run('test.csv', 0, mock.MagicMock(), 0, '')

        mock_db_manager.return_value.delete_invoices.assert_not_called()
        mock_db_manager.return_value.insert_invoices.assert_called_once()

    @mock.patch(f'{_ONLINE_RETAIL_MODULE}.TransactionsDBManager')
    @mock.patch(f'{_CSV_READER_CLASS}.read_transactions', lambda *args: None)
    def test_run_should_delete_from_db_mode_is_delete(self, mock_db_manager):

        online_retail.Runner.run('test.csv', 0, mock.MagicMock(), 0, 'delete')

        mock_db_manager.return_value.delete_invoices.assert_called_once()
        mock_db_manager.return_value.insert_invoices.assert_not_called()
