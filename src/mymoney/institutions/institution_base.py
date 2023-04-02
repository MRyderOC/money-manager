import json
import logging
import dataclasses
from enum import Enum
from typing import List, Dict, Union

import numpy as np
import pandas as pd
from importlib_resources import files

from mymoney.utils.data_validation import DataFrameValidation

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


@dataclasses.dataclass
class TransformedData:
    """docs here!"""
    sanity_df: pd.DataFrame
    output_df: pd.DataFrame
    out_type: str


class ServiceType(Enum):
    BASE = "base"
    CREDIT = "credit"
    DEBIT = "debit"
    THIRDPARTY = "3rdparty"
    EXCHANGE = "exchange"


class Service():
    """docs here!"""

    _service_type = ServiceType("base")

    def __init__(self, inst_meta_data: Dict) -> None:
        md = inst_meta_data.get(self._service_type.value)
        if not md:
            raise Exception(
                f"No `{self._service_type.value}` service available in inst_meta_data."
            )
        self._this_meta_data = md


    def _data_validation(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """docs here!"""
        val_data = self._this_meta_data.get("validation_data")
        if not val_data:
            raise Exception("Validation data is not available.")

        schema = val_data.get("schema")
        col_vals = val_data.get("column_values")

        df_validate_instance = DataFrameValidation(input_df)
        if schema:
            df_validate_instance.has_schema(schema)
        if col_vals:
            input_df["_new_IsValid"] = df_validate_instance.has_vals(
                col_vals_dict=col_vals, return_validation_col=True
            )

        return input_df


    def _cleaning(self, input_df: pd.DataFrame, account_name: str) -> pd.DataFrame:
        """...

        Args:
            input_df (pd.DataFrame):
                ...
            account_name (str):
                ...

        Returns:
            ...
        """
        ...


    def create_sanity_data(self, input_df: pd.DataFrame, account_name: str) -> pd.DataFrame:
        """docs here!"""
        return self._data_validation(self._cleaning(input_df, account_name))


class Institution():
    """docs here!"""

    _this_institution_name = "base"

    _USDs = ["USD", "USDC", "USDT"]
    _new_expense_columns = [
        "Description", "Amount", "Date", "Institution",
        "InstitutionCategory", "MyCategory",
        "IsTransfer", "IsValid",
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
        self._meta_data = json.loads(
            files("mymoney").joinpath("meta_data.json").read_text()
        )
        self._this_meta_data = self._meta_data.get(self._this_institution_name)


    def _output_df_creator(self, df: pd.DataFrame) -> pd.DataFrame:
        """docs here!"""
        new_columns_name_map = {
            col: col[5:]
            for col in df.columns
            if col.startswith("_new_")
        }
        old_columns = [
            col
            for col in df.columns
            if not col.startswith("_new_")
        ]

        return df.drop(columns=old_columns).rename(columns=new_columns_name_map)


    def service_executer(
        self,
        input_df: pd.DataFrame,
        service_name: str,
        account_name: str
    ) -> TransformedData:
        """docs here!"""
        if service_name == "debit":
            the_service = self.DebitService(self._this_meta_data)
            out_type = "balance"
        elif service_name == "credit":
            the_service = self.CreditService(self._this_meta_data)
            out_type = "expense"
        elif service_name == "3rdparty":
            the_service = self.ThirdPartyService(self._this_meta_data)
            out_type = "expense"
        elif service_name == "exchange":
            the_service = self.ExchangeService(self._this_meta_data)
            out_type = "trade"
        else:
            raise ValueError(
                "service_name should be one of the following:"
                " 'debit', 'credit', '3rdparty', 'exchange'."
            )

        sanity_df = the_service.create_sanity_data(input_df, account_name)
        out_df = self._output_df_creator(sanity_df)
        return TransformedData(
            sanity_df=sanity_df,
            output_df=out_df,
            out_type=out_type,
        )


    class CreditService(Service):
        """Prototype class for Credit Service."""
        _service_type = ServiceType("credit")
        ...

    class DebitService(Service):
        """Prototype class for Debit Service."""
        _service_type = ServiceType("debit")
        ...

    class ThirdPartyService(Service):
        """Prototype class for ThirdParty Service."""
        _service_type = ServiceType("3rdparty")
        ...

    class ExchangeService(Service):
        """Prototype class for Exchange Service."""
        _service_type = ServiceType("exchange")
        ...