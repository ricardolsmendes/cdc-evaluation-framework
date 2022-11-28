import unittest
from unittest import mock

import cdc_eval
from cdc_eval import cdc_eval_cli


class CDCEvalCLITest(unittest.TestCase):
    __CLI_MODULE = 'cdc_eval.cdc_eval_cli'
    __CLI_CLASS = f'{__CLI_MODULE}.CDCEvalCLI'

    @mock.patch(f'{__CLI_CLASS}._parse_args')
    def test_run_should_parse_args(self, mock_parse_args):
        cdc_eval_cli.CDCEvalCLI.run([])
        mock_parse_args.assert_called_once()

    @mock.patch(f'{__CLI_CLASS}._CDCEvalCLI__use_kaggle_online_retail_uci_ds')
    @mock.patch(f'{__CLI_CLASS}._parse_args')
    def test_run_should_call_worker_method(self, mock_parse_args,
                                           mock_use_kaggle_online_retail_uci_ds):

        mock_parse_args.return_value.func = mock_use_kaggle_online_retail_uci_ds
        cdc_eval_cli.CDCEvalCLI.run([])
        mock_use_kaggle_online_retail_uci_ds.assert_called_once_with(
            mock_parse_args.return_value)

    def test_parse_args_no_subcommand_should_raise_system_exit(self):
        self.assertRaises(SystemExit, cdc_eval_cli.CDCEvalCLI._parse_args,
                          ['--data-file', 'test.csv'])

    def test_parse_args_invalid_subcommand_should_raise_system_exit(self):
        self.assertRaises(SystemExit, cdc_eval_cli.CDCEvalCLI._parse_args, ['kaggle'])

    def test_parse_args_kaggle_online_retail_uci_missing_mandatory_args_should_raise_system_exit(  # noqa E501
            self):

        self.assertRaises(SystemExit, cdc_eval_cli.CDCEvalCLI._parse_args,
                          ['kaggle-online-retail-uci'])

    def test_parse_args_kaggle_online_retail_uci_should_parse_mandatory_args(self):
        args = cdc_eval_cli.CDCEvalCLI._parse_args([
            'kaggle-online-retail-uci', '--data-file', 'test.csv', '--db-conn',
            'test-conn'
        ])
        self.assertEqual('test.csv', args.data_file)
        self.assertEqual('test-conn', args.db_conn)

    def test_parse_args_kaggle_online_retail_uci_should_parse_optional_args(self):
        args = cdc_eval_cli.CDCEvalCLI._parse_args([
            'kaggle-online-retail-uci', '--data-file', 'test.csv', '--invoices', '10',
            '--db-conn', 'test-conn', '--operation-delay', '3', '--operation-mode',
            'delete'
        ])
        self.assertEqual('10', args.invoices)
        self.assertEqual('3', args.operation_delay)
        self.assertEqual('delete', args.operation_mode)

    @mock.patch(f'{__CLI_CLASS}._CDCEvalCLI__use_kaggle_online_retail_uci_ds')
    def test_parse_args_kaggle_online_retail_uci_should_set_default_function(
            self, mock_use_kaggle_online_retail_uci_ds):

        args = cdc_eval_cli.CDCEvalCLI._parse_args([
            'kaggle-online-retail-uci', '--data-file', 'test.csv', '--db-conn',
            'test-conn'
        ])
        self.assertEqual(mock_use_kaggle_online_retail_uci_ds, args.func)

    @mock.patch(f'{__CLI_MODULE}.kaggle_online_retail_ii_uci.Runner')
    def test_use_kaggle_online_retail_uci_ds_should_insert_tags_from_csv(
            self, mock_runner):
        cdc_eval_cli.CDCEvalCLI.run([
            'kaggle-online-retail-uci', '--data-file', 'test.csv', '--db-conn',
            'test-conn'
        ])
        mock_runner.run.assert_called_with(data_file='test.csv',
                                           invoices=0,
                                           db_conn='test-conn',
                                           operation_delay=1.0,
                                           operation_mode='insert')

    @mock.patch(f'{__CLI_CLASS}.run')
    def test_main_should_call_cli_run(self, mock_run):
        cdc_eval.main()
        mock_run.assert_called_once()
