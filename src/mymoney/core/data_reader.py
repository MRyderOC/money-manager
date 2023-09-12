import os
import json
import logging
from typing import List, Dict, Any

import pandas as pd
from importlib_resources import files

from mymoney.core.data_classes import InstData
from mymoney.institutions.institution_base import DataType
from mymoney.utils.common import column_name_checker


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)

pd.options.mode.chained_assignment = None


class DataReader:
    """Main class for reading data."""

    def __init__(self) -> None:
        self._meta_data = json.loads(
            files("mymoney").joinpath("meta_data.json").read_text()
        )

    def _path_is_csv_like(self, path: str) -> bool:
        """Check whether the `path` is a path to csv file.

        Args:
            path (str):
                The input path.

        Returns:
            True if `path` is a csv and False otherwise.
        """
        _, ext = os.path.splitext(path)
        if ext.casefold() == ".csv":
            return True
        return False

    def _read_wellsfargo_csv(
            self, path: str, account_name: str, read_args: Dict[str, Any]
    ) -> InstData:
        """Read the CSV data from `path` and returns a InstData. This method
        is specialized for WellsFargo CSV files since they need special care.

        Args:
            path (str):
                The path to read the csv file from.
            account_name (str):
                The name of the account to be used.
                If it's None the name of the file will be used.
            read_args (Dict[str, Any]):
                Read arguments for pd.read_csv.

        Returns:
            An InstData object.
        """
        def is_wellsfargo(input_df: pd.DataFrame) -> bool:
            """Check whether the `input_df` is a WellsFargo DataFrame.

            Args:
                input_df (pd.DataFrame):
                    The input DataFrame.

            Returns:
                True if `input_df` is from WellsFargo institution.
            """

            try:
                check_wellsfargo = (
                    "Unnamed: 3" in input_df.columns,
                    "*" in input_df.columns,
                    (input_df["Unnamed: 3"].isna()).all(),
                    (input_df["*"] == "*").all(),
                )
            except KeyError:
                return False

            if not all(check_wellsfargo):
                return False

            return True

        if not is_wellsfargo(pd.read_csv(path)):
            return None

        input_df = pd.read_csv(filepath_or_buffer=path, **read_args)

        wf_service_cond = any(
            input_df["Description"].str.contains(r"PAYMENT\s?-? THANK")
        )
        wf_service = "credit" if wf_service_cond else "debit"

        logging.info(f"Completed: wellsfargo/{wf_service} - {account_name}")
        return InstData(
            source=path,
            data_type=DataType.CSV,
            institution_name="wellsfargo",
            service_name=wf_service,
            account_name=account_name,
            table=input_df,
        )

    def read_csv(
            self, path: str, account_name: str = None, logs: bool = False
    ) -> InstData:
        """Read the data from `path` and returns a InstData.

        Args:
            path (str):
                The path to read the csv file from.
            account_name (str):
                The name of the account to be used.
                If it's None the name of the file will be used.
            logs (bool):
                Show the logs for failed attempt of reading.

        Returns:
            An InstData object.
        """
        if not self._path_is_csv_like(path):
            raise ValueError("`path` should point to a CSV file.")

        for institution in self._meta_data:
            for service in self._meta_data[institution]:
                name = f"{institution}/{service}"
                cols = self._meta_data[institution][service]["columns"]
                read_args = self._meta_data[institution][service]["read_args"]
                if account_name is None:
                    account_name = os.path.basename(path).split(".")[0]

                # Read the data
                read_flag = False
                error_msg = ""
                try:
                    input_df = pd.read_csv(
                        filepath_or_buffer=path, **read_args)
                    column_name_checker(input_df, cols, "subset")
                    read_flag = True
                except Exception as err:
                    error_msg = err
                    if institution == "wellsfargo":
                        wf_inst = self._read_wellsfargo_csv(
                            path=path, account_name=account_name,
                            read_args=read_args
                        )
                        if wf_inst is not None:
                            return wf_inst

                if not read_flag:
                    if logs:
                        msg = f"An error occurred for {name}: {error_msg}"
                        logging.warning(msg)
                    continue

                logging.info(f"Completed: {name} - {account_name}")
                return InstData(
                    source=path,
                    data_type=DataType.CSV,
                    institution_name=institution,
                    service_name=service,
                    account_name=account_name,
                    table=input_df,
                )

        # Log a warning if data can not be read
        logging.warning(f"Couldn't read the data for {path}")

    def t_read_csv(self, folder_path: str) -> List[InstData]:
        """Traverse `folder_path` and returns a list that contains
        InstData for each csv file in the `folder_path`. This method should be
        used to traverse at most one level deep. If there is a folder
        inside `folder_path`, the name of the that folder will be considered
        as the account_name for the csv files in that folder.

        Args:
            folder_path (str):
                The folder's path to read the csv files from.

        Returns:
            A list of InstData objects.
        """
        out_list = []

        for dirpath, _, filenames in os.walk(folder_path):
            # Find the account_name if needed
            account_name = None
            if dirpath != folder_path:
                account_name = os.path.basename(dirpath)

            for filename in filenames:
                if not self._path_is_csv_like(filename):
                    continue

                tmp_path = os.path.join(dirpath, filename)
                if read_data := self.read_csv(tmp_path, account_name):
                    out_list.append(read_data)

        return out_list
