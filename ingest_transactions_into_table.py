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
This application demonstrates how to read retail transactions' data from a CSV
file and ingest them into a database table.
"""

import argparse
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
        logging.info('Reading the transactions file...')
        df = pd.read_csv(file)
        logging.info('DONE!')

        print()
        print(df.head())
        print()

        logging.info('  > Dataframe shape: %s', df.shape)
        logging.info('  > Dataframe index: %s', df.index)
        logging.info('  > Dataframe columns: %s', df.columns)
        logging.info('==================================================')

        return df


"""
Database table writer
========================================
"""


class DBTableWriter:

    def __init__(self, db_conn_string: str):
        self.__db_conn_string = db_conn_string

    def write_invoices(self, transactions: DataFrame,
                       write_delay: float) -> None:

        logging.info('')
        logging.info('Looking for unique invoice numbers...')
        invoice_numbers = transactions['Invoice'].unique()
        logging.info('  > %d found', len(invoice_numbers))

        logging.info('')
        logging.info('Connecting to the database...')
        con = self.create_db_connection()

        logging.info('')
        logging.info('Ingesting invoices...')

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

        for invoice in invoice_numbers:
            logging.info('')
            invoice_items = db_like_df[db_like_df['invoice'] == invoice]
            logging.info('  Ingesting invoice "%s" with %d items...', invoice,
                         len(invoice_items))

            print()
            print(invoice_items.head())
            print()

            affected_lines = invoice_items.to_sql(name='transactions',
                                                  con=con,
                                                  if_exists='append',
                                                  index=False)
            logging.info('  > %d lines affected', affected_lines)

            time.sleep(write_delay)

        logging.info('DONE!')
        logging.info('==================================================')

        return

    def create_db_connection(self) -> Engine:
        return sqlalchemy.create_engine(self.__db_conn_string)


"""
Main program entry point
========================================
"""
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Ingest transactions data')

    parser.add_argument('--data-file', help='the CSV data file', required=True)
    parser.add_argument('--db-conn',
                        help='the database connection string for SQLAlchemy',
                        required=True)
    parser.add_argument(
        '--write-delay',
        help='seconds to wait between database write operations',
        default=1)

    args = parser.parse_args()

    transactions_df = CSVFilesReader.read_transactions(args.data_file)
    db_table_writer = DBTableWriter(args.db_conn)
    db_table_writer.write_invoices(transactions=transactions_df,
                                   write_delay=float(args.write_delay))
