import os  # noqa: F401
import logging
from typing import Dict

import numpy as np  # noqa: F401
import pandas as pd

from mymoney.analysis.expense import ExpenseAnalysis
from mymoney.storage_integrations import SheetsOperations


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class MyData:
    """Main class for storing MyMoney core data and analysis."""

    # Main DataFrames
    expense_df: pd.DataFrame = pd.DataFrame()
    trade_df: pd.DataFrame = pd.DataFrame()
    # balance_df: pd.DataFrame = pd.DataFrame()

    # Storage Integrations
    sheets_creds: str | Dict = None
    sheet_name: str = "MyMoney"
    # folder_path: str = os.path.join(os.environ["HOME"], "mymoney")
    # TODO: drive_folder_path: str = None

    def __init__(
        self,
        sheets_creds: str | Dict = None,
        sheet_name: str = "MyMoney",
        # folder_path: str = os.path.join(os.environ["HOME"], "mymoney"),
    ):
        if sheets_creds:
            self.sheets_op = SheetsOperations(sheets_creds, sheet_name)

    def read_sheets(self):
        self.expense_df = self.sheets_op.expenses_to_df()
        self.trade_df = self.sheets_op.trades_to_df()

        self._load_analysis_instances()

    def _load_analysis_instances(self):
        self.expense_analysis = ExpenseAnalysis(self.expense_df)
        # self.trade_analysis = TradeAnalysis(self.trade_df)

    # def read_my_folder(self):
    #     fo = folder_operations.FolderOperations(root_path=self.folder_path)
    #     data = fo.read_my_folder()
    #     ...
