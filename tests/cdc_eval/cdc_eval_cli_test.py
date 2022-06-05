import unittest
from unittest import mock

import cdc_eval


class CDCEvalCLITest(unittest.TestCase):
    __CLI_MODULE = 'cdc_eval.cdc_eval_cli'
    __CLI_CLASS = f'{__CLI_MODULE}.CDCEvalCLI'

    @mock.patch(f'{__CLI_CLASS}.run')
    def test_main_should_call_cli_run(self, mock_run):
        cdc_eval.main()
        mock_run.assert_called_once()
