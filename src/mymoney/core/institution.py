import json
import logging
from typing import Dict, List

import numpy as np
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class Institution():
    """docs here!"""

    _meta_data_path = "meta_data.json"
    _this_institution_name = "base"

    _USDs = ["USD", "USDC", "USDT"]
    _new_expense_columns = [
        "Description", "Amount", "Date",
        "InstitutionCategory", "MyCategory",
        "Institution", "IsTransfer", "IsCompatible"
    ]
    _new_trade_columns = [
        "Datetime",
        "From Account", "To Account",
        "From Asset", "To Asset",
        "In Amount", "Out Amount",
        "Fee Asset", "Fee Amount", "Fee Value",
        "Trx Type", "Trx Sub Type",
        "Asset Type",
        "USD Amount",
    ]


    def __init__(self) -> None:
        with open(self._meta_data_path) as f:
            self._meta_data = json.load(f)
        self._this_meta_data = self._meta_data.get(self._this_institution_name)


    def service_executer(
        self,
        input_df: pd.DataFrame,
        service_name: str,
        account_name: str
    ) -> Dict[str, pd.DataFrame]:
        if service_name == "debit":
            out_dict = self.debit(input_df, account_name)
        elif service_name == "credit":
            out_dict = self.credit(input_df, account_name)
        elif service_name == "3rdparty":
            out_dict = self.third_party(input_df, account_name)
        elif service_name == "exchange":
            out_dict = self.exchange(input_df, account_name)
        else:
            raise ValueError(
                "service_name should be one of the following:"
                " 'debit', 'credit', '3rdparty', 'exchange'."
            )

        return out_dict


    def _credit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """Prototype function that each subclass of Institution should implement if they have `credit` services."""
        ...

    def _debit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """Prototype function that each subclass of Institution should implement if they have `debit` services."""
        ...

    def _third_party_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """Prototype function that each subclass of Institution should implement if they have `3rdparty` services."""
        ...

    def _exchange_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """Prototype function that each subclass of Institution should implement if they have `exchange` services."""
        ...


    def debit(
        self, input_df: pd.DataFrame, account_name: str
    ) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        clean_df = self._debit_cleaning(input_df, account_name)
        # Error/Type checking in here if needed
        return {
            "expense": clean_df
        }

    def credit(
        self, input_df: pd.DataFrame, account_name: str
    ) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        clean_df = self._credit_cleaning(input_df, account_name)
        # Error/Type checking in here if needed
        return {
            "expense": clean_df
        }

    def third_party(
        self, input_df: pd.DataFrame, account_name: str
    ) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        clean_df = self._third_party_cleaning(input_df, account_name)
        # Error/Type checking in here if needed
        return {
            "expense": clean_df
        }

    def exchange(
        self, input_df: pd.DataFrame, account_name: str
    ) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        clean_df = self._exchange_cleaning(input_df, account_name)
        # Error/Type checking in here if needed
        return {
            "trade": clean_df
        }
