import os
import logging
from typing import Dict

import numpy as np
import pandas as pd

from mymoney.core import raw_data_reader
from mymoney.core import output_transformer


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
        self._data_reader = raw_data_reader.RawDataReader()
        self._output_trnsfrmr = output_transformer.OutputTransformer()
        pass


    def path_to_whole_data_dict(self, path: str, account_name: str = None) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        input_data = self._data_reader.data_reader(path)
        if account_name:
            input_data["account_name"] = account_name
        out_dict = self._output_trnsfrmr.institution_executer(**input_data)
        input_data.update(out_dict)
        input_data.update({
            "path": path,
        })
        return input_data


    def traversal(self, folder_path: str = "./csv_files") -> Dict[str, pd.DataFrame]:
        """Traverse csv_files folder by default and
        return two DataFrames each for trades and expenses.
        Note: This function does not traverse inner folders.
        """
        all_outs_as_list = []

        for dirname, _, filenames in os.walk(folder_path):
            if dirname == folder_path:
                for filename in filenames:
                    try:
                        name, ext = filename.split(".")
                    except ValueError:
                        raise OSError(
                            f"{filename} doesn't have any extension."
                        )
                    if ext.casefold() != "csv":
                        continue

                    path = os.path.join(dirname, filename)
                    out_dict = self.path_to_whole_data_dict(path, account_name=name)
                    all_outs_as_list.append(out_dict)

        return all_outs_as_list
