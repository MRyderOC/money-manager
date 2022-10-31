import os
import logging
import dataclasses
from typing import Dict, Union

import numpy as np
import pandas as pd

from mymoney.core import data_classes
from mymoney.core import raw_data_reader


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class ExecClass():
    """docs here!"""
    # TODO: ETL Pipeline

    def __init__(self) -> None:
        pass


    def path_to_whole_data(self, path: str, account_name: str = None) -> data_classes.WholeData:
        """docs here!"""
        data_reader_instance = raw_data_reader.RawDataReader()
        input_data = data_reader_instance.data_reader(path)
        if account_name:
            input_data.account_name = account_name
        transformed_data = input_data.institution_executer()

        return data_classes.WholeData(
            **{
                **dataclasses.asdict(input_data),
                **dataclasses.asdict(transformed_data)
            }
        )


    def traversal(self, folder_path: str = "./csv_files") -> Dict[str, pd.DataFrame]:
        """Traverse csv_files folder by default and
        return two DataFrames each for trades and expenses.
        Note: This function does not traverse inner folders.
        """
        all_outs_as_list = []

        for dirname, _, filenames in os.walk(folder_path):
            if dirname == folder_path:
                for filename in filenames:
                    name, ext = os.path.splitext(filename)
                    if ext.casefold() != ".csv":
                        continue

                    path = os.path.join(dirname, filename)
                    whole_data = self.path_to_whole_data(path, account_name=name)
                    all_outs_as_list.append(whole_data)

        return all_outs_as_list
