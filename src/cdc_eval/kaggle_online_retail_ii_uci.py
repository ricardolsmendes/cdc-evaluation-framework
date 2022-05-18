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
"""
This application is used to read retail transactions' data from a CSV file and
insert or delete them into/from a database table.

A fully working dataset can be downloaded from
https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci.
"""

import logging
import time

import pandas as pd
from pandas import DataFrame
import sqlalchemy
from sqlalchemy.engine import Engine
"""
Input reader
========================================
"""


class CSVFilesReader:

    @classmethod
    def read_transactions(cls, file: str) -> DataFrame:
        logging.info('')
        logging.info('Reading the transactions file...')
        df = pd.read_csv(file)
        logging.info('DONE!')

        PandasHelper.print_df_metadata(df)

        return df


"""
Transactions database manager
========================================
"""


class TransactionsDBManager:

    def __init__(self, db_conn_string: str):
        self.__db_conn_string = db_conn_string

    def delete_invoices(self, transactions: DataFrame,
                        operation_delay: float) -> None:
        return

    def insert_invoices(self, transactions: DataFrame,
                        operation_delay: float) -> None:

        logging.info('')
        logging.info('Connecting to the database...')
        con = self.create_db_connection()

        logging.info('')
        logging.info('Inserting invoices...')

        db_like_df = transactions.rename(
            columns={
                'Invoice': 'invoice',
                'StockCode': 'stock_code',
                'Description': 'description',
                'Quantity': 'quantity',
                'InvoiceDate': 'invoice_date',
                'Price': 'price',
                'Customer ID': 'customer_id',
                'Country': 'country'
            })

        for invoice in transactions['Invoice'].unique():
            logging.info('')
            invoice_items = db_like_df[db_like_df['invoice'] == invoice]
            logging.info('  Inserting invoice "%s" with %d items...', invoice,
                         len(invoice_items))

            print()
            print(invoice_items.head())
            print()

            affected_lines = invoice_items.to_sql(name='transactions',
                                                  con=con,
                                                  if_exists='append',
                                                  index=False)
            logging.info('  > %d lines affected', affected_lines)

            time.sleep(operation_delay)

        logging.info('DONE!')
        logging.info('==================================================')

        return

    def create_db_connection(self) -> Engine:
        return sqlalchemy.create_engine(self.__db_conn_string)


"""
Pandas helper
========================================
"""


class PandasHelper:

    @classmethod
    def print_df_metadata(cls, df: DataFrame) -> None:
        print()
        logging.info('  > Dataframe shape: %s', df.shape)
        logging.info('  > Dataframe index: %s', df.index)
        logging.info('  > Dataframe columns: %s', df.columns)

        print()
        print('  > Dataframe head:')
        print(df.head())
        print()

        logging.info('==================================================')

    @classmethod
    def select_random_subsets(cls, df: DataFrame, id_column: str,
                              n: int) -> DataFrame:

        logging.info('')
        logging.info('Selecting %d random subsets...', n)
        unique_subset_ids = cls.select_unique_values(df, id_column)
        random_subset_ids = cls.select_random_items(unique_subset_ids, n)

        random_subsets = pd.DataFrame()

        for _, row in random_subset_ids.iterrows():
            subset_id = row[id_column]
            subset = df[df[id_column] == subset_id]
            random_subsets = pd.concat([random_subsets, subset])

        logging.info('DONE!')

        cls.print_df_metadata(random_subsets)

        return random_subsets

    @classmethod
    def select_random_items(cls, df: DataFrame, n: int) -> DataFrame:
        logging.info('')
        logging.info('Selecting %d random items...', n)
        random_items = df.sample(n)
        logging.info('DONE!')

        cls.print_df_metadata(random_items)

        return random_items

    @classmethod
    def select_unique_values(cls, df: DataFrame, column: str) -> DataFrame:
        logging.info('')
        logging.info('Selecting unique values for "%s"...', column)
        unique_values = df[column].unique()
        logging.info('  > %d found', len(unique_values))
        logging.info('DONE!')

        unique_values_df = pd.DataFrame(unique_values, columns=[column])
        cls.print_df_metadata(unique_values_df)

        return unique_values_df


"""
Main module entry point
========================================
"""


class Runner:

    @classmethod
    def run(cls, data_file: str, invoices: int, db_conn: str,
            operation_delay: float, operation_mode: str) -> None:

        transactions_df = CSVFilesReader.read_transactions(data_file)

        if invoices > 0:
            transactions_df = PandasHelper.select_random_subsets(
                transactions_df, 'Invoice', invoices)

        transactions_db_mgr = TransactionsDBManager(db_conn)

        # The script operation mode defaults to `insert`.
        if operation_mode is 'delete':
            transactions_db_mgr.delete_invoices(
                transactions=transactions_df, operation_delay=operation_delay)
        else:
            transactions_db_mgr.insert_invoices(
                transactions=transactions_df, operation_delay=operation_delay)
