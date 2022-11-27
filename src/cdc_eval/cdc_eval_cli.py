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

import argparse
import logging
import sys

from cdc_eval import kaggle_online_retail_ii_uci


class CDCEvalCLI:

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()

        args = cls._parse_args(argv)
        args.func(args)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

        subparsers = parser.add_subparsers()

        kaggle_online_retail_uci_parser = subparsers.add_parser(
            'kaggle-online-retail-uci',
            help='Use the "Kaggle Online Retail II UCI" dataset')
        kaggle_online_retail_uci_parser.add_argument('--data-file',
                                                     help='the CSV data file',
                                                     required=True)
        kaggle_online_retail_uci_parser.add_argument('--invoices',
                                                     help='the number of invoices',
                                                     default=0)
        kaggle_online_retail_uci_parser.add_argument(
            '--db-conn',
            help='the database connection string for SQLAlchemy',
            required=True)
        kaggle_online_retail_uci_parser.add_argument(
            '--operation-delay',
            help='seconds to wait between database operations',
            default=1)
        kaggle_online_retail_uci_parser.add_argument(
            '--operation-mode',
            help='the script operation mode: insert or delete',
            default='insert')

        kaggle_online_retail_uci_parser.set_defaults(
            func=cls.__use_kaggle_online_retail_uci_ds)

        return parser.parse_args(argv)

    @classmethod
    def __use_kaggle_online_retail_uci_ds(cls, args):
        kaggle_online_retail_ii_uci.Runner().run(data_file=args.data_file,
                                                 invoices=int(args.invoices),
                                                 db_conn=args.db_conn,
                                                 operation_delay=float(
                                                     args.operation_delay),
                                                 operation_mode=args.operation_mode)


"""
Main program entry point
========================================
"""


def main():
    argv = sys.argv
    CDCEvalCLI.run(argv[1:] if len(argv) > 0 else argv)
