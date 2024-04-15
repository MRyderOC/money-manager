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

    # Storage Integrations
    sheets_creds: str | Dict = None
    sheet_name: str = "MyMoney"

    def __init__(
        self,
        sheets_creds: str | Dict = None,
        sheet_name: str = "MyMoney",
    ):
        if sheets_creds:
            self.sheets_op = SheetsOperations(sheets_creds, sheet_name)

    def read_sheets(self):
        self.expense_df = self.sheets_op.expenses_to_df()
        self.trade_df = self.sheets_op.trades_to_df()

        self._load_analysis_instances()

    def _load_analysis_instances(self):
        self.expense_analysis = ExpenseAnalysis(self.expense_df)
