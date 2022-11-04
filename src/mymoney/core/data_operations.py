import os
import shutil
import logging
from datetime import datetime
from typing import Dict

import numpy as np
import pandas as pd

from mymoney.core import data_classes


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class DataOperations():
    """docs here!"""

    _expense_columns = [
        "Description", "Amount", "Date",
        "InstitutionCategory", "MyCategory",
        "Institution", "IsTransfer",# "IsCompatible"
    ]
    _trade_columns = [
        "Datetime",
        "FromAccount", "ToAccount",
        "FromAsset", "ToAsset",
        "InAmount", "OutAmount",
        "FeeAsset", "FeeAmount", "FeeValue",
        "TrxType", "TrxSubType",
        "AssetType", "USDAmount",
    ]
    _balance_columns = [
        "FirstTestCol", "SecondTestCol"
    ]


    def __init__(self, data_folder_path: str = None) -> None:
        # Set the data_folder_path to `<HOME-DIR>/mymoney/data` as default
        if not data_folder_path:
            self._data_folder_path = os.path.join(os.environ["HOME"], "mymoeny/data")
            _ = self._is_data_folder_structure_exists()
        else:
            if self._is_data_folder_structure_exists(data_folder_path):
                if os.path.basename(data_folder_path) != "data":
                    data_folder_path = os.path.join(data_folder_path, "data")
                self._data_folder_path = data_folder_path

        self._core_folder_path = os.path.join(self._data_folder_path, "core")
        self._raw_folder_path = os.path.join(self._data_folder_path, "raw")
        self._sanity_folder_path = os.path.join(self._data_folder_path, "sanity")

        self._expense_csv_path = os.path.join(self._core_folder_path, "expense.csv")
        self._trade_csv_path = os.path.join(self._core_folder_path, "trade.csv")
        self._balance_csv_path = os.path.join(self._core_folder_path, "balance.csv")


    def _is_data_folder_structure_exists(
        self, path: str = None, raises: bool = False, log: bool = True
    ) -> bool:
        """docs here!"""
        if not path:
            path = self._data_folder_path

        if os.path.basename(path) != "data":
            data_folder_path = os.path.join(path, "data")
        else:
            data_folder_path = path
        core_folder_path = os.path.join(data_folder_path, "core")
        raw_folder_path = os.path.join(data_folder_path, "raw")
        sanity_folder_path = os.path.join(data_folder_path, "sanity")

        if not(
            os.path.exists(data_folder_path)
            or os.path.exists(core_folder_path)
            or os.path.exists(raw_folder_path)
            or os.path.exists(sanity_folder_path)
        ):
            error_msg = (
                f"The path `{path}` doesn't have the data folder structure."
                " Use `initiate_data_folder` method to create the correct one."
            )
            if raises:
                raise FileNotFoundError(error_msg)
            if log:
                logging.error(error_msg)

            return False

        return True


    def initiate_data_folder(self):
        """docs here!

        Create `data` folder with `core`, `raw` and `sanity_check` in it.
        Create 3 csv files with each specific headers.
        """
        if self._is_data_folder_structure_exists(log=False):
            raise Exception(
                "data folder exists with correct structure."
                " No need to initiate it again."
            )
        # Creating the folders
        os.makedirs(self._core_folder_path, exist_ok=True)
        os.makedirs(self._raw_folder_path, exist_ok=True)
        os.makedirs(self._sanity_folder_path, exist_ok=True)

        # Creating the core csv files
        pd.DataFrame(columns=self._expense_columns).to_csv(self._expense_csv_path, index=False)
        pd.DataFrame(columns=self._trade_columns).to_csv(self._trade_csv_path, index=False)
        pd.DataFrame(columns=self._balance_columns).to_csv(self._balance_csv_path, index=False)


    def load_db(self) -> data_classes.MyData:
        """Read csv files from core folder and return corresponding dataframes."""
        expense = pd.read_csv(
            self._expense_csv_path, parse_dates=["Date"]
        ).sort_values("Date").reset_index(drop=True)
        trade = pd.read_csv(
            self._trade_csv_path, parse_dates=["Datetime"]
        ).sort_values("Datetime").reset_index(drop=True)
        # TODO: parse_date for balance
        balance = pd.read_csv(self._balance_csv_path)

        return data_classes.MyData(
            expense=expense,
            trade=trade,
            balance=balance
        )


    def append_to_db(self, data: data_classes.WholeData):
        """Get the WholeData and append to the right core data."""
        self._is_data_folder_structure_exists(raises=True)

        if data.out_type == "expense":
            data.output_df.to_csv(self._expense_csv_path, mode="a", header=False, index=False)
        elif data.out_type == "trade":
            data.output_df.to_csv(self._trade_csv_path, mode="a", header=False, index=False)
        elif data.out_type == "balance":
            data.output_df.to_csv(self._balance_csv_path, mode="a", header=False, index=False)


    def store_raw_data(self, data: data_classes.WholeData, remove_source: bool = False):
        """docs here!"""
        self._is_data_folder_structure_exists(raises=True)

        # Create a folder to store the data
        current_time = datetime.today().strftime('%Y-%m-%d')
        folder_path = os.path.join(self._raw_folder_path, current_time)
        os.makedirs(folder_path, exist_ok=True)
        # Store the data
        file_name = data.generate_file_name()
        target_path = os.path.join(folder_path, f"{file_name}.csv")
        shutil.copyfile(data.path, target_path)
        # Remove the source data if needed
        if remove_source:
            os.remove(data.path)


    def store_sanity_data(self, data: data_classes.WholeData):
        """docs here!"""
        self._is_data_folder_structure_exists(raises=True)

        # Create a folder to store the data
        current_time = datetime.today().strftime('%Y-%m-%d')
        folder_path = os.path.join(self._sanity_folder_path, current_time)
        os.makedirs(folder_path, exist_ok=True)
        # Store the data
        file_name = data.generate_file_name()
        target_path = os.path.join(folder_path, f"{file_name}.csv")
        data.sanity_df.to_csv(target_path)
