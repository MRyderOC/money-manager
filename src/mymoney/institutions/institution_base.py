import json
import logging
from enum import Enum
from typing import Dict

import pandas as pd
from importlib_resources import files

from mymoney.utils.data_validation import DataFrameValidation


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class DataType(Enum):
    """Enum class for compatible data types to process."""
    CSV = "csv"
    PDF = "pdf"
    ANY = "any"


class ServiceType(Enum):
    """Enum class for different service types for each institution."""
    BASE = "base"
    CREDIT = "credit"
    DEBIT = "debit"
    THIRDPARTY = "3rdparty"
    EXCHANGE = "exchange"


class Service:
    """Base class for each service of an institution."""

    _service_type = ServiceType.BASE

    def __init__(self, inst_meta_data: Dict) -> None:
        md = inst_meta_data.get(self._service_type.value)
        if not md:
            raise Exception(
                f"No `{self._service_type.value}`"
                " service available in inst_meta_data."
            )
        self._this_meta_data = md

    def _data_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Creates the `IsValid` column for the `df` based on the data
        available in meta_data for this specific service.

        Args:
            df (pd.DataFrame):
                The input DataFrame.

        Returns:
            The same DataFrame with a new column `_new_IsValid`.
        """
        val_data = self._this_meta_data.get("validation_data")
        if not val_data:
            raise Exception("Validation data is not available.")

        schema = val_data.get("schema")
        col_vals = val_data.get("column_values")

        df_validate_instance = DataFrameValidation(df)
        if schema:
            df_validate_instance.has_schema(schema)
        if col_vals:
            df["_new_IsValid"] = df_validate_instance.has_vals(
                col_vals_dict=col_vals, return_validation_col=True
            )

        return df

    def _csv_cleaning(
            self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """Prototype method for cleaning process of CSV files for this service.
        Subclasses should implement this method.

        Args:
            input_df (pd.DataFrame):
                The input DataFrame.
            account_name (str):
                The name of the account associated with this service.

        Returns:
            The same DataFrame with new columns for cleaned data.

        Raises:
            NotImplementedError:
                If the subclass hasn't implemented this method.
        """
        raise NotImplementedError("Subclass should implement this method!")

    def data_type_exec(
            self, data_type: DataType, table: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """Execute the cleaning function corresponded to `data_type`
        and validate the results.

        Args:
            data_type (DataType):
                The type of input data.
                Refer to `DataType` for supported types.
            table (pd.DataFrame):
                The input table as a DataFrame.
            account_name (str):
                The name of the account associated with this service.

        Returns:
            The same DataFrame with new columns for cleaned data.
        """
        match data_type:
            case DataType.CSV: df = self._csv_cleaning(
                    input_df=table, account_name=account_name
            )
            case DataType.PDF: raise NotImplementedError("Not supported yet!")
            case DataType.ANY: raise NotImplementedError("Not supported yet!")

        return self._data_validation(df)


class Institution:
    """Base class for institutions to do
    data transformation related operations."""

    _this_institution_name = "base"
    _USDs = ["USD", "USDC", "USDT"]

    def __init__(self) -> None:
        self._meta_data = json.loads(
            files("mymoney").joinpath("meta_data.json").read_text()
        )
        self._this_meta_data = self._meta_data.get(self._this_institution_name)

    def service_executer(
        self,
        service_name: str,
        data_type: DataType,
        table: pd.DataFrame,
        account_name: str
    ) -> pd.DataFrame:
        """Execute the cleaning function corresponded to `service_name`.

        Args:
            service_name (str):
                The service of the input data.
                Refer to `ServiceType` for supported services.
            data_type (DataType):
                The type of the input data.
                Refer to `DataType` for supported types.
            table (pd.DataFrame):
                The input table as a DataFrame.
            account_name (str):
                The name of the account associated with this service.

        Returns:
            The same DataFrame with new columns for cleaned data.
        """
        md = self._this_meta_data
        match service_name:
            case "debit": the_serv = self.DebitService(md)
            case "credit": the_serv = self.CreditService(md)
            case "3rdparty": the_serv = self.ThirdPartyService(md)
            case "exchange": the_serv = self.ExchangeService(md)
            case _:
                raise ValueError(
                    "service_name should be one of the following:"
                    " ['debit', 'credit', '3rdparty', 'exchange']."
                )

        return the_serv.data_type_exec(
            data_type=data_type, table=table, account_name=account_name
        )

    class CreditService(Service):
        """Prototype class for Credit Service."""
        _service_type = ServiceType.CREDIT

    class DebitService(Service):
        """Prototype class for Debit Service."""
        _service_type = ServiceType.DEBIT

    class ThirdPartyService(Service):
        """Prototype class for ThirdParty Service."""
        _service_type = ServiceType.THIRDPARTY

    class ExchangeService(Service):
        """Prototype class for Exchange Service."""
        _service_type = ServiceType.EXCHANGE
