import os
import json
import logging
from typing import List

import pandas as pd
from importlib_resources import files

from mymoney.core.data_classes import InstData
from mymoney.institutions.institution_base import DataType
from mymoney.utils.exceptions import DifferentColumnNameException


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class DataReader:
    """Main class for reading data."""

    def __init__(self) -> None:
        self._meta_data = json.loads(
            files("mymoney").joinpath("meta_data.json").read_text()
        )

    def _column_name_checker(
        self, input_df: pd.DataFrame, columns: List[str]
    ):
        """Check if the columns of `input_df` is identical to `columns`.

        Args:
            input_df (pd.DataFrame):
                The input DataFrame.
            columns (List[str]):
                Columns that `input_df` columns will be compared to.

        Raises:
            DifferentColumnNameException: if the columns don't match
        """
        if set(input_df.columns) != set(columns):
            raise DifferentColumnNameException(
                "DataFrame columns are not matched with `columns`."
            )

    def _is_wellsfargo(self, input_df: pd.DataFrame) -> bool:
        """Check whether the `input_df` is a WellsFargo DataFrame.

        Args:
            input_df (pd.DataFrame):
                The input DataFrame.

        Returns:
            True if `input_df` is from WellsFargo institution.
        """

        check_wellsfargo = (
            "Unnamed: 3" in input_df.columns and "*" in input_df.columns,
            (input_df["Unnamed: 3"].isna()).all(),
            (input_df["*"] == "*").all(),
        )
        if not all(check_wellsfargo):
            return False

        return True

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

    def read_csv(self, path: str, logs: bool = False) -> InstData:
        """Read the data from `path` and returns a InstData.

        Args:
            path (str):
                The path to read the csv file from.
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
                account_name = os.path.basename(path).split(".")[0]

                # Read the data
                read_flag = False
                error_msg = ""
                try:
                    input_df = pd.read_csv(
                        filepath_or_buffer=path, **read_args)
                    self._column_name_checker(input_df, cols)
                    read_flag = True
                except Exception as err:
                    error_msg = err
                    if institution == "wellsfargo":
                        if self._is_wellsfargo(pd.read_csv(path)):
                            input_df = pd.read_csv(
                                filepath_or_buffer=path, **read_args)
                            read_flag = True

                if read_flag:
                    logging.info(f"Completed: {name} - {account_name}")
                    return InstData(
                        source=path,
                        data_type=DataType.CSV,
                        institution_name=institution,
                        service_name=service,
                        account_name=account_name,
                        table=input_df,
                    )
                else:
                    if logs:
                        logging.warning(
                            f"An error occurred for {name}: {error_msg}")
                    continue

        # Log a warning if data can not be read
        logging.warning(f"Couldn't read the data for {path}")

    def t_read_csv(self, folder_path: str) -> List[InstData]:
        """Traverse `folder_path` and returns a list that contains
        InstData for each csv file in the folder_path.
        Note: This function does not traverse inner folders.

        Args:
            folder_path (str):
                The folder's path to read the csv files from.

        Returns:
            A list of InstData objects.
        """
        out_list = []

        for dirname, _, filenames in os.walk(folder_path):
            if dirname != folder_path:
                continue

            for filename in filenames:
                if not self._path_is_csv_like(filename):
                    continue

                out_list.append(
                    self.read_csv(os.path.join(dirname, filename))
                )

        return out_list
